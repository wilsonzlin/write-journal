use dashmap::DashMap;
use off64::usz;
use off64::Off64Int;
use off64::Off64Slice;
use rustc_hash::FxHasher;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::WriteRequest;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::hash::BuildHasherDefault;
use std::sync::atomic::AtomicU64;
use tokio::time::sleep;
use tracing::info;
use tracing::warn;

const OFFSETOF_HASH: u64 = 0;
const OFFSETOF_LEN: u64 = OFFSETOF_HASH + 32;
const OFFSETOF_ENTRIES: u64 = OFFSETOF_LEN + 4;

pub struct Transaction {
  serial_no: u64,
  writes: Vec<(u64, Vec<u8>)>,
}

impl Transaction {
  fn serialised_byte_len(&self) -> u64 {
    u64::try_from(self.writes.iter().map(|w| 8 + 4 + w.1.len()).sum::<usize>()).unwrap()
  }

  pub fn write(&mut self, offset: u64, data: Vec<u8>) -> &mut Self {
    self.writes.push((offset, data));
    self
  }
}

pub struct WriteJournal {
  device: SeekableAsyncFile,
  offset: u64,
  capacity: u64,
  pending: DashMap<u64, (Transaction, SignalFutureController), BuildHasherDefault<FxHasher>>,
  next_txn_serial_no: AtomicU64,
}

impl WriteJournal {
  pub fn new(device: SeekableAsyncFile, offset: u64, capacity: u64) -> Self {
    assert!(capacity > OFFSETOF_ENTRIES && capacity <= u32::MAX.into());
    Self {
      device,
      offset,
      capacity,
      pending: Default::default(),
      next_txn_serial_no: AtomicU64::new(0),
    }
  }

  pub fn generate_blank_state(&self) -> Vec<u8> {
    let mut raw = vec![0u8; usz!(OFFSETOF_ENTRIES)];
    raw.write_u32_be_at(OFFSETOF_LEN, 0u32);
    let hash = blake3::hash(raw.read_slice_at_range(OFFSETOF_LEN..));
    raw.write_slice_at(OFFSETOF_HASH, hash.as_bytes());
    raw
  }

  pub async fn format_device(&self) {
    self
      .device
      .write_at(self.offset, self.generate_blank_state())
      .await;
  }

  pub async fn recover(&self) {
    let mut raw = self.device.read_at(self.offset, OFFSETOF_ENTRIES).await;
    let len: u64 = raw.read_u32_be_at(OFFSETOF_LEN).into();
    if len > self.capacity - OFFSETOF_ENTRIES {
      warn!("journal is corrupt, has invalid length, skipping recovery");
      return;
    };
    raw.append(
      &mut self
        .device
        .read_at(self.offset + OFFSETOF_ENTRIES, len.into())
        .await,
    );
    let expected_hash = blake3::hash(raw.read_slice_at_range(OFFSETOF_LEN..));
    let recorded_hash = raw.read_slice_at_range(..OFFSETOF_LEN);
    if expected_hash.as_bytes() != recorded_hash {
      warn!("journal is corrupt, has invalid hash, skipping recovery");
      return;
    };
    if len == 0 {
      info!("journal is empty, no recovery necessary");
      return;
    };
    let mut recovered_bytes_total = 0;
    let mut journal_offset = OFFSETOF_ENTRIES;
    while journal_offset < len {
      let offset = raw.read_u64_be_at(journal_offset);
      journal_offset += 8;
      let data_len = raw.read_u32_be_at(journal_offset);
      journal_offset += 4;
      let data = raw
        .read_slice_at_range(journal_offset..journal_offset + u64::from(data_len))
        .to_vec();
      journal_offset += u64::from(data_len);
      self.device.write_at(offset, data).await;
      recovered_bytes_total += data_len;
    }
    self
      .device
      .write_at(self.offset, self.generate_blank_state())
      .await;
    self.device.sync_data().await;
    info!(
      recovered_entries = len,
      recovered_bytes = recovered_bytes_total,
      "journal has been recovered"
    );
  }

  pub fn begin_transaction(&self) -> Transaction {
    let serial_no = self
      .next_txn_serial_no
      .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Transaction {
      serial_no,
      writes: Vec::new(),
    }
  }

  pub async fn commit(&self, txn: Transaction) {
    let (fut, fut_ctl) = SignalFuture::new();
    let None = self.pending.insert(txn.serial_no, (txn, fut_ctl)) else {
      unreachable!();
    };
    fut.await;
  }

  pub async fn start_commit_background_loop(&self) {
    let mut next_serial = 0;

    loop {
      sleep(std::time::Duration::from_micros(200)).await;

      let mut len = 0;
      let mut raw = vec![0u8; usz!(OFFSETOF_ENTRIES)];
      let mut writes = Vec::new();
      let mut fut_ctls = Vec::new();
      // We must `remove` to take ownership of the write data and avoid copying. But this means we need to reinsert into the map if we cannot process a transaction in this iteration.
      while let Some((serial_no, (txn, fut_ctl))) = self.pending.remove(&next_serial) {
        let entry_len = txn.serialised_byte_len();
        if len + entry_len > self.capacity - OFFSETOF_ENTRIES {
          // Out of space, wait until next iteration.
          let None = self.pending.insert(serial_no, (txn, fut_ctl)) else {
            unreachable!();
          };
          break;
        };
        next_serial += 1;
        for (offset, data) in txn.writes {
          let data_len: u32 = data.len().try_into().unwrap();
          raw.extend_from_slice(&offset.to_be_bytes());
          raw.extend_from_slice(&data_len.to_be_bytes());
          raw.extend_from_slice(&data);
          len += entry_len;
          writes.push(WriteRequest::new(offset, data));
        }
        fut_ctls.push(fut_ctl);
      }
      if fut_ctls.is_empty() {
        continue;
      };
      raw.write_u32_be_at(OFFSETOF_LEN, u32::try_from(len).unwrap());
      let hash = blake3::hash(raw.read_slice_at_range(OFFSETOF_LEN..));
      raw.write_slice_at(OFFSETOF_HASH, hash.as_bytes());
      self
        .device
        .write_at_with_delayed_sync(vec![WriteRequest::new(self.offset, raw)])
        .await;

      self.device.write_at_with_delayed_sync(writes).await;

      for fut_ctl in fut_ctls {
        fut_ctl.signal();
      }

      // We cannot write_at_with_delayed_sync, as we may write to the journal again by then and have a conflict due to reordering.
      self
        .device
        .write_at(self.offset, self.generate_blank_state())
        .await;
      self.device.sync_data().await;
    }
  }
}
