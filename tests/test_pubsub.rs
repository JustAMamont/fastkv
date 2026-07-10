//! Tests for Pub/Sub functionality.

#[cfg(test)]
mod tests {
    use fast_kv::core::pubsub::PubSubRegistry;

    #[tokio::test]
    async fn test_subscribe_and_publish() {
        let registry = PubSubRegistry::new();
        let mut rx = registry.subscribe("test_channel").await;
        
        let n = registry.publish("test_channel", b"hello world".to_vec()).await;
        assert_eq!(n, 1, "one subscriber should receive");
        
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.channel, "test_channel");
        assert_eq!(msg.message, b"hello world");
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let registry = PubSubRegistry::new();
        let mut rx1 = registry.subscribe("ch").await;
        let mut rx2 = registry.subscribe("ch").await;
        
        let n = registry.publish("ch", b"broadcast".to_vec()).await;
        assert_eq!(n, 2, "two subscribers should receive");
        
        assert_eq!(rx1.recv().await.unwrap().message, b"broadcast");
        assert_eq!(rx2.recv().await.unwrap().message, b"broadcast");
    }

    #[tokio::test]
    async fn test_publish_no_subscribers() {
        let registry = PubSubRegistry::new();
        let n = registry.publish("ghost_channel", b"msg".to_vec()).await;
        assert_eq!(n, 0, "no subscribers = 0 receivers");
    }

    #[tokio::test]
    async fn test_list_channels() {
        let registry = PubSubRegistry::new();
        let _rx1 = registry.subscribe("channel_a").await;
        let _rx2 = registry.subscribe("channel_b").await;
        
        let channels = registry.list_channels(None).await;
        assert_eq!(channels.len(), 2);
        assert!(channels.contains(&"channel_a".to_string()));
        assert!(channels.contains(&"channel_b".to_string()));
    }

    #[tokio::test]
    async fn test_list_channels_glob() {
        let registry = PubSubRegistry::new();
        let _rx1 = registry.subscribe("pump:alerts").await;
        let _rx2 = registry.subscribe("pump:dex_alerts").await;
        let _rx3 = registry.subscribe("other_channel").await;
        
        let channels = registry.list_channels(Some("pump:*")).await;
        assert_eq!(channels.len(), 2);
        assert!(channels.contains(&"pump:alerts".to_string()));
        assert!(channels.contains(&"pump:dex_alerts".to_string()));
    }

    #[tokio::test]
    async fn test_numsub() {
        let registry = PubSubRegistry::new();
        let _rx1 = registry.subscribe("ch1").await;
        let _rx2 = registry.subscribe("ch1").await;
        let _rx3 = registry.subscribe("ch2").await;
        
        assert_eq!(registry.numsub("ch1").await, 2);
        assert_eq!(registry.numsub("ch2").await, 1);
        assert_eq!(registry.numsub("nonexistent").await, 0);
    }

    #[tokio::test]
    async fn test_cleanup_empty_channel() {
        let registry = PubSubRegistry::new();
        {
            let _rx = registry.subscribe("temp").await;
            assert_eq!(registry.numsub("temp").await, 1);
        }
        // rx dropped — cleanup
        registry.cleanup_channel("temp").await;
        let channels = registry.list_channels(None).await;
        assert!(!channels.contains(&"temp".to_string()));
    }

    #[tokio::test]
    async fn test_total_subscribers() {
        let registry = PubSubRegistry::new();
        assert_eq!(registry.total_subscribers(), 0);
        
        let _rx1 = registry.subscribe("a").await;
        let _rx2 = registry.subscribe("b").await;
        assert_eq!(registry.total_subscribers(), 2);
    }
}
