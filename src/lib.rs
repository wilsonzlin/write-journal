use off64::usz;
use off64::Off64Int;
use off64::Off64Slice;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::WriteRequest;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::collections::VecDeque;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::info;
use tracing::warn;

const OFFSETOF_HASH: u64 = 0;
const OFFSETOF_LEN: u64 = OFFSETOF_HASH + 32;
const OFFSETOF_ENTRIES: u64 = OFFSETOF_LEN + 4;

pub struct AtomicWriteGroup(pub Vec<(u64, Vec<u8>)>);

impl AtomicWriteGroup {
  fn serialised_byte_len(&self) -> u64 {
    u64::try_from(self.0.iter().map(|w| 8 + 4 + w.1.len()).sum::<usize>()).unwrap()
  }
}

pub struct WriteJournal {
  device: SeekableAsyncFile,
  offset: u64,
  capacity: u64,
  pending: Mutex<VecDeque<(AtomicWriteGroup, SignalFutureController)>>,
}

impl WriteJournal {
  pub fn new(device: SeekableAsyncFile, offset: u64, capacity: u64) -> Self {
    assert!(capacity > OFFSETOF_ENTRIES && capacity <= u32::MAX.into());
    Self {
      device,
      offset,
      capacity,
      pending: Mutex::new(VecDeque::new()),
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

  // Sometimes we want to ensure a bunch of writes at different offsets are atomically written as one, such that either all writes or none persist and not some of the writes only.
  pub async fn write(&self, atomic_group: AtomicWriteGroup) {
    let (fut, fut_ctl) = SignalFuture::new();
    self.enqueue_with_custom_signal(atomic_group, fut_ctl).await;
    fut.await;
  }

  // For advanced usages only. This is async only to acquire the internal lock, not to actually perform commit.
  pub async fn enqueue_with_custom_signal(
    &self,
    atomic_group: AtomicWriteGroup,
    fut_ctl: SignalFutureController,
  ) {
    self
      .enqueue_many_with_custom_signal(vec![(atomic_group, fut_ctl)])
      .await
  }

  // For advanced usages only. This is async only to acquire the internal lock, not to actually perform commits. More efficient as it only acquires lock once.
  pub async fn enqueue_many_with_custom_signal(
    &self,
    writes: Vec<(AtomicWriteGroup, SignalFutureController)>,
  ) {
    let mut pending = self.pending.lock().await;
    for w in writes {
      assert!(w.0.serialised_byte_len() <= self.capacity - OFFSETOF_ENTRIES);
      pending.push_back(w);
    }
  }

  pub async fn start_commit_background_loop(&self) {
    loop {
      sleep(std::time::Duration::from_micros(200)).await;

      let mut len = 0;
      let mut raw = vec![0u8; usz!(OFFSETOF_ENTRIES)];
      let mut writes = Vec::new();
      let mut fut_ctls = Vec::new();
      {
        let mut pending = self.pending.lock().await;
        while let Some((group, fut_ctl)) = pending.pop_front() {
          let entry_len = group.serialised_byte_len();
          if len + entry_len > self.capacity - OFFSETOF_ENTRIES {
            pending.push_front((group, fut_ctl));
            break;
          };
          for (offset, data) in group.0 {
            let data_len: u32 = data.len().try_into().unwrap();
            raw.extend_from_slice(&offset.to_be_bytes());
            raw.extend_from_slice(&data_len.to_be_bytes());
            raw.extend_from_slice(&data);
            len += entry_len;
            writes.push(WriteRequest::new(offset, data));
          }
          fut_ctls.push(fut_ctl);
        }
      };
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
