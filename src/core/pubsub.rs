//! Pub/Sub — channel-based publish/subscribe messaging.
//!
//! Supports SUBSCRIBE, UNSUBSCRIBE, PUBLISH, and PUBSUB CHANNELS commands.
//! Uses `tokio::sync::broadcast` for fan-out to multiple subscribers.

use std::collections::HashMap;
use tokio::sync::{broadcast, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};

/// Broadcast channel capacity per Pub/Sub channel.
const CHANNEL_CAPACITY: usize = 256;

/// A message published to a channel.
#[derive(Clone, Debug)]
pub struct PubSubMessage {
    pub channel: String,
    pub message: Vec<u8>,
}

/// Global Pub/Sub registry shared across all connections.
pub struct PubSubRegistry {
    /// channel name → broadcast sender
    channels: RwLock<HashMap<String, broadcast::Sender<PubSubMessage>>>,
    /// Monotonic subscriber counter
    total_subscribers: AtomicU64,
}

impl Default for PubSubRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSubRegistry {
    pub fn new() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
            total_subscribers: AtomicU64::new(0),
        }
    }

    /// Get or create a broadcast sender for a channel.
    async fn get_or_create_sender(&self, channel: &str) -> broadcast::Sender<PubSubMessage> {
        let mut channels = self.channels.write().await;
        channels
            .entry(channel.to_string())
            .or_insert_with(|| {
                let (tx, _rx) = broadcast::channel(CHANNEL_CAPACITY);
                tx
            })
            .clone()
    }

    /// Subscribe to a channel. Returns a broadcast receiver.
    pub async fn subscribe(&self, channel: &str) -> broadcast::Receiver<PubSubMessage> {
        let sender = self.get_or_create_sender(channel).await;
        self.total_subscribers.fetch_add(1, Ordering::Relaxed);
        sender.subscribe()
    }

    /// Clean up a channel if it has no subscribers.
    pub async fn cleanup_channel(&self, channel: &str) {
        let mut channels = self.channels.write().await;
        if let Some(sender) = channels.get(channel)
            && sender.receiver_count() == 0 {
                channels.remove(channel);
            }
        self.total_subscribers.fetch_sub(1, Ordering::Relaxed);
    }

    /// Publish a message to a channel. Returns number of receivers.
    pub async fn publish(&self, channel: &str, message: Vec<u8>) -> usize {
        let channels = self.channels.read().await;
        if let Some(sender) = channels.get(channel) {
            if sender.receiver_count() == 0 {
                return 0;
            }
            let msg = PubSubMessage {
                channel: channel.to_string(),
                message,
            };
            sender.send(msg).unwrap_or(0)
        } else {
            0
        }
    }

    /// List active channels with at least one subscriber.
    pub async fn list_channels(&self, pattern: Option<&str>) -> Vec<String> {
        let channels = self.channels.read().await;
        let mut result: Vec<String> = channels
            .iter()
            .filter(|(_, sender)| sender.receiver_count() > 0)
            .map(|(name, _)| name.clone())
            .collect();
        if let Some(p) = pattern {
            result.retain(|name| matches_glob(name, p));
        }
        result.sort();
        result
    }

    /// Count subscribers for a specific channel.
    pub async fn numsub(&self, channel: &str) -> usize {
        let channels = self.channels.read().await;
        channels.get(channel).map(|s| s.receiver_count()).unwrap_or(0)
    }

    /// Total subscriber count.
    pub fn total_subscribers(&self) -> u64 {
        self.total_subscribers.load(Ordering::Relaxed)
    }
}

/// Simple glob matching (supports * wildcard).
fn matches_glob(text: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        text.starts_with(prefix)
    } else if let Some(suffix) = pattern.strip_prefix('*') {
        text.ends_with(suffix)
    } else {
        text == pattern
    }
}
