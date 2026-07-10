//! Tests for Sorted Set functionality.

#[cfg(test)]
mod tests {
    use fast_kv::core::sortedset::SortedSetStore;

    #[tokio::test]
    async fn test_zadd_and_zcard() {
        let store = SortedSetStore::new();
        
        let added = store.zadd("myset", &[
            (1.0, b"a".to_vec()),
            (2.0, b"b".to_vec()),
            (3.0, b"c".to_vec()),
        ]).await;
        assert_eq!(added, 3);
        assert_eq!(store.zcard("myset").await, 3);
    }

    #[tokio::test]
    async fn test_zadd_update_existing() {
        let store = SortedSetStore::new();
        
        store.zadd("s", &[(1.0, b"x".to_vec())]).await;
        let added = store.zadd("s", &[(5.0, b"x".to_vec())]).await;
        assert_eq!(added, 0, "update existing = 0 new");
        assert_eq!(store.zcard("s").await, 1);
        assert_eq!(store.zscore("s", b"x").await, Some(5.0));
    }

    #[tokio::test]
    async fn test_zscore() {
        let store = SortedSetStore::new();
        store.zadd("s", &[(42.0, b"answer".to_vec())]).await;
        
        assert_eq!(store.zscore("s", b"answer").await, Some(42.0));
        assert_eq!(store.zscore("s", b"nonexistent").await, None);
        assert_eq!(store.zscore("nonexistent_set", b"x").await, None);
    }

    #[tokio::test]
    async fn test_zrange_ascending() {
        let store = SortedSetStore::new();
        store.zadd("s", &[
            (3.0, b"three".to_vec()),
            (1.0, b"one".to_vec()),
            (2.0, b"two".to_vec()),
        ]).await;
        
        let result = store.zrange("s", 0, -1).await;
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], b"one");
        assert_eq!(result[1], b"two");
        assert_eq!(result[2], b"three");
    }

    #[tokio::test]
    async fn test_zrange_subset() {
        let store = SortedSetStore::new();
        store.zadd("s", &[
            (1.0, b"a".to_vec()),
            (2.0, b"b".to_vec()),
            (3.0, b"c".to_vec()),
            (4.0, b"d".to_vec()),
            (5.0, b"e".to_vec()),
        ]).await;
        
        let result = store.zrange("s", 1, 3).await;
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], b"b");
        assert_eq!(result[2], b"d");
    }

    #[tokio::test]
    async fn test_zrevrange_descending() {
        let store = SortedSetStore::new();
        store.zadd("s", &[
            (1.0, b"a".to_vec()),
            (2.0, b"b".to_vec()),
            (3.0, b"c".to_vec()),
        ]).await;
        
        let result = store.zrevrange("s", 0, -1).await;
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], b"c");  // highest score first
        assert_eq!(result[1], b"b");
        assert_eq!(result[2], b"a");
    }

    #[tokio::test]
    async fn test_zrevrangebyscore() {
        let store = SortedSetStore::new();
        store.zadd("s", &[
            (10.0, b"a".to_vec()),
            (20.0, b"b".to_vec()),
            (30.0, b"c".to_vec()),
            (40.0, b"d".to_vec()),
            (50.0, b"e".to_vec()),
        ]).await;
        
        let result = store.zrevrangebyscore("s", 40.0, 20.0).await;
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], b"d");  // 40
        assert_eq!(result[1], b"c");  // 30
        assert_eq!(result[2], b"b");  // 20
    }

    #[tokio::test]
    async fn test_zrem() {
        let store = SortedSetStore::new();
        store.zadd("s", &[
            (1.0, b"a".to_vec()),
            (2.0, b"b".to_vec()),
            (3.0, b"c".to_vec()),
        ]).await;
        
        let removed = store.zrem("s", &[b"b".to_vec(), b"nonexistent".to_vec()]).await;
        assert_eq!(removed, 1);
        assert_eq!(store.zcard("s").await, 2);
        assert_eq!(store.zscore("s", b"b").await, None);
    }

    #[tokio::test]
    async fn test_zincrby() {
        let store = SortedSetStore::new();
        store.zadd("s", &[(10.0, b"counter".to_vec())]).await;
        
        let new_score = store.zincrby("s", 5.0, b"counter").await;
        assert_eq!(new_score, 15.0);
        assert_eq!(store.zscore("s", b"counter").await, Some(15.0));
    }

    #[tokio::test]
    async fn test_zincrby_new_member() {
        let store = SortedSetStore::new();
        let new_score = store.zincrby("s", 3.0, b"new_member").await;
        assert_eq!(new_score, 3.0);
        assert_eq!(store.zcard("s").await, 1);
    }

    #[tokio::test]
    async fn test_del_sorted_set() {
        let store = SortedSetStore::new();
        store.zadd("s", &[(1.0, b"a".to_vec())]).await;
        assert!(store.exists("s").await);
        
        assert!(store.del("s").await);
        assert!(!store.exists("s").await);
        assert_eq!(store.zcard("s").await, 0);
    }

    #[tokio::test]
    async fn test_negative_indices() {
        let store = SortedSetStore::new();
        store.zadd("s", &[
            (1.0, b"a".to_vec()),
            (2.0, b"b".to_vec()),
            (3.0, b"c".to_vec()),
        ]).await;
        
        // -1 = last element
        let result = store.zrange("s", -1, -1).await;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], b"c");
        
        // -2 to -1 = last two
        let result = store.zrange("s", -2, -1).await;
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_empty_set_operations() {
        let store = SortedSetStore::new();
        assert_eq!(store.zcard("nonexistent").await, 0);
        assert_eq!(store.zrange("nonexistent", 0, -1).await.len(), 0);
        assert_eq!(store.zrevrange("nonexistent", 0, -1).await.len(), 0);
        assert_eq!(store.zrem("nonexistent", &[b"x".to_vec()]).await, 0);
    }

    #[tokio::test]
    async fn test_nan_score() {
        let store = SortedSetStore::new();
        let added = store.zadd("s", &[(f64::NAN, b"nan_member".to_vec())]).await;
        assert_eq!(added, 1, "NaN score should still be addable");
        assert_eq!(store.zcard("s").await, 1);
    }

    #[tokio::test]
    async fn test_negative_scores() {
        let store = SortedSetStore::new();
        store.zadd("s", &[
            (-10.0, b"neg".to_vec()),
            (0.0, b"zero".to_vec()),
            (10.0, b"pos".to_vec()),
        ]).await;
        
        let result = store.zrange("s", 0, -1).await;
        assert_eq!(result[0], b"neg");   // -10 first
        assert_eq!(result[1], b"zero");  // 0
        assert_eq!(result[2], b"pos");   // 10 last
    }
}
