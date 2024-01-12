#![allow(dead_code)]

pub mod asynchronous;

use std::borrow::Borrow;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard};
use utils::BkdrHasher;

use crate::cache::{Cache, KeyPtr};
use crate::AfterRemovedFnMut;

const NUM_SHARD_BITS: usize = 4;
const NUM_SHARDS: usize = 1 << NUM_SHARD_BITS;

#[derive(Debug)]
pub struct ShardedCache<K: Display, V: Debug> {
    shard: [Arc<Mutex<Cache<K, V>>>; NUM_SHARDS],
}

impl<K, V> Default for ShardedCache<K, V>
where
    K: Eq + Hash + Display,
    V: Debug,
{
    fn default() -> Self {
        Self::with_capacity(128)
    }
}

impl<K, V> ShardedCache<K, V>
where
    K: Eq + Hash + Display,
    V: Debug,
{
    pub fn with_capacity(capacity: usize) -> ShardedCache<K, V> {
        // FIXME: Cannot set a precise capacity freely (such as 1000, will be 63 * 16)
        let per_shard = (capacity + (NUM_SHARDS - 1)) / NUM_SHARDS;
        Self {
            shard: unsafe {
                let shard = MaybeUninit::<[Arc<Mutex<Cache<K, V>>>; NUM_SHARDS]>::uninit();
                let mut shard = shard.assume_init();
                for e in shard.iter_mut() {
                    ptr::write(e, Arc::new(Mutex::new(Cache::with_capacity(per_shard))));
                }
                shard
            },
        }
    }

    fn shard<Q>(k: &Q) -> usize
    where
        KeyPtr<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut hasher = BkdrHasher::new();
        k.hash(&mut hasher);
        let hash = hasher.finish();
        (hash % NUM_SHARDS as u64) as usize
    }

    pub fn lock_shard<'a, Q>(&'a self, k: &Q) -> CacheMutexGuard<'a, K, V>
    where
        KeyPtr<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        CacheMutexGuard {
            inner: self.shard[Self::shard(k)].lock(),
        }
    }

    pub fn remove<Q>(&self, k: &Q)
    where
        KeyPtr<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.shard[Self::shard(k)].lock().remove(k)
    }

    pub fn prune(&self) {
        for s in self.shard.iter() {
            s.lock().prune();
        }
    }
}

pub struct CacheMutexGuard<'a, K: Display, V: Debug> {
    inner: MutexGuard<'a, Cache<K, V>>,
}

impl<K, V> CacheMutexGuard<'_, K, V>
where
    K: Eq + Hash + Display,
    V: Debug,
{
    pub fn insert(&mut self, k: K, v: V) -> Option<&V> {
        self.insert_opt(k, v, 1, None)
    }

    pub fn insert_opt(
        &mut self,
        k: K,
        v: V,
        charge: usize,
        after_removed: Option<AfterRemovedFnMut<K, V>>,
    ) -> Option<&V> {
        self.inner
            .insert_and_return_value(k, v, charge, after_removed)
            .map(|v| unsafe { &(*v).v })
    }

    pub fn get<Q>(&mut self, k: &Q) -> Option<&V>
    where
        KeyPtr<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.get_value(k).map(|v| unsafe { &(*v).v })
    }

    pub fn get_mut<Q>(&mut self, k: &Q) -> Option<&mut V>
    where
        KeyPtr<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.get_value_mut(k).map(|v| unsafe { &mut (*v).v })
    }

    pub fn remove<Q>(&mut self, k: &Q)
    where
        KeyPtr<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.remove(k)
    }

    pub fn prune(&mut self) {
        self.inner.prune();
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{self, AtomicUsize};
    use std::sync::{Arc, Barrier};

    use parking_lot::RwLock;
    use tokio::runtime::Runtime;

    use crate::sharded::ShardedCache;

    /// Visit all keys from the cache in parallel, and return the hit/miss/error count.
    /// If the key is not in the cache, insert new key-value into the cache.
    fn read_cache_in_parallel(
        cache: Arc<ShardedCache<String, Arc<String>>>,
        keys: Arc<Vec<String>>,
        task_count: usize,
        runtime: Arc<Runtime>,
    ) -> (usize, usize, usize) {
        let hit_cnt = Arc::new(AtomicUsize::new(0));
        let miss_cnt = Arc::new(AtomicUsize::new(0));
        let error_cnt = Arc::new(AtomicUsize::new(0));

        let mut key_i_vec = (0..keys.len()).collect::<Vec<usize>>();
        key_i_vec.sort_by(|_, _| rand::random::<i8>().cmp(&0));

        let mut tasks = Vec::with_capacity(task_count);
        let task_key_range_base = keys.len() / task_count;
        let task_barrier = Arc::new(Barrier::new(task_count));
        for i in 0..task_count {
            let keys = keys.clone();
            let cache = cache.clone();
            let hit_cnt = hit_cnt.clone();
            let miss_cnt = miss_cnt.clone();
            let error_cnt = error_cnt.clone();
            let runtime = runtime.clone();

            let task_barrier = Arc::clone(&task_barrier);
            let jh = std::thread::spawn(move || {
                let key_i_start = task_key_range_base * i;
                let key_i_end = task_key_range_base * (i + 1);
                task_barrier.wait();

                // Start task: [{key_i_start}, {key_i_end})
                for key_i in key_i_start..key_i_end {
                    let key = keys[key_i].clone();
                    let val = Arc::new(format!("val_{key_i}"));
                    let mut cache = cache.lock_shard(&key);
                    println!("Getting key: {key}");
                    let val_ref = match cache.get(&key) {
                        Some(v) => {
                            println!("Got key: {key}");
                            hit_cnt.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            std::thread::sleep(std::time::Duration::from_millis(100));
                            v.clone()
                        }
                        None => {
                            miss_cnt.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            // Mock: take some time to init the value (open a file, etc.)
                            std::thread::sleep(std::time::Duration::from_micros(100));
                            match cache.get(&key) {
                                Some(v) => {
                                    println!("Got key before insert: {key}");
                                    v.clone()
                                }
                                None => {
                                    println!("Insert new key: {key}");
                                    match cache.insert(key.clone(), val.clone()) {
                                        // match cache.insert_opt(
                                        //     key.clone(),
                                        //     val.clone(),
                                        //     1,
                                        //     Some(Box::new(|k: &String, v: &mut Arc<String>| {
                                        //         println!("Auto delete: {k} -> {v}");
                                        //     })),
                                        // ) {
                                        Some(v) => v.clone(),
                                        None => {
                                            eprintln!("Value of key: {} failed to insert", key);
                                            error_cnt
                                                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                            continue;
                                        }
                                    }
                                }
                            }
                        }
                    };
                    assert_eq!(val.as_str(), val_ref.as_str(), "key_of_val: {key}");
                    runtime.spawn(async move {
                        // Hold the value for a while.
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        let _p = val_ref.as_ptr();
                    });
                }
                // End task: [{key_i_start}, {key_i_end})"
            });
            tasks.push(jh);
        }
        for t in tasks {
            if let Err(e) = t.join() {
                eprintln!("Error(in): {:?}", e);
                error_cnt.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        (
            hit_cnt.load(atomic::Ordering::SeqCst),
            miss_cnt.load(atomic::Ordering::SeqCst),
            error_cnt.load(atomic::Ordering::SeqCst),
        )
    }

    #[test]
    fn test_read_write_in_parallel() {
        const KEY_NUM: usize = 128;
        const CACHE_CAPACITY: usize = 16;
        const TASK_NUM: usize = 8;
        const SUB_TASK_NUM: usize = 8;

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_time()
            .build()
            .unwrap();
        let runtime = Arc::new(runtime);

        let mut keys: Vec<String> = Vec::with_capacity(KEY_NUM);
        for i in 0..keys.capacity() {
            keys.push(format!("key_{i}"));
        }
        let keys = Arc::new(keys);

        let cache: Arc<ShardedCache<String, Arc<String>>> =
            Arc::new(ShardedCache::with_capacity(CACHE_CAPACITY));

        let hit_cnt = Arc::new(AtomicUsize::new(0));
        let miss_cnt = Arc::new(AtomicUsize::new(0));
        let error_cnt = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::with_capacity(TASK_NUM);
        for _ in 0..TASK_NUM {
            let keys = keys.clone();
            let cache = cache.clone();
            let hit_cnt = hit_cnt.clone();
            let miss_cnt = miss_cnt.clone();
            let error_cnt = miss_cnt.clone();
            let runtime = runtime.clone();
            let jh = std::thread::spawn(move || {
                let (hit, miss, err) = read_cache_in_parallel(cache, keys, SUB_TASK_NUM, runtime);
                hit_cnt.fetch_add(hit, atomic::Ordering::SeqCst);
                miss_cnt.fetch_add(miss, atomic::Ordering::SeqCst);
                error_cnt.fetch_add(err, atomic::Ordering::SeqCst);
            });
            tasks.push(jh);
        }
        for t in tasks {
            if let Err(e) = t.join() {
                println!("Error(out): {:?}", e);
            }
        }
        println!(
            "Total hit: {}, Total missed: {}, Total errors: {}",
            hit_cnt.load(std::sync::atomic::Ordering::SeqCst),
            miss_cnt.load(std::sync::atomic::Ordering::SeqCst),
            error_cnt.load(std::sync::atomic::Ordering::SeqCst),
        );
    }

    const CACHE_SIZE: usize = 1000;

    struct ShardedCacheTester {
        cache: ShardedCache<i32, i32>,
        deleted_keys: Arc<RwLock<Vec<i32>>>,
        deleted_values: Arc<RwLock<Vec<i32>>>,
    }

    impl ShardedCacheTester {
        fn new() -> Self {
            Self {
                cache: ShardedCache::with_capacity(CACHE_SIZE),
                deleted_keys: Arc::new(RwLock::new(Vec::new())),
                deleted_values: Arc::new(RwLock::new(Vec::new())),
            }
        }

        fn get_or_default(&mut self, key: i32) -> i32 {
            if let Some(v) = self.cache.lock_shard(&key).get(&key) {
                *v
            } else {
                -1
            }
        }

        fn insert(&mut self, key: i32, value: i32) {
            Self::insert_charge(self, key, value, 1);
        }

        fn insert_charge(&mut self, key: i32, value: i32, charge: i32) {
            self.cache.lock_shard(&key).insert_opt(
                key,
                value,
                charge as usize,
                Some(Box::new(fn_deleter(
                    self.deleted_keys.clone(),
                    self.deleted_values.clone(),
                ))),
            );
        }

        fn insert_and_return_default(&mut self, key: i32, value: i32) -> i32 {
            if let Some(v) = self.cache.lock_shard(&key).insert_opt(
                key,
                value,
                1,
                Some(Box::new(fn_deleter(
                    self.deleted_keys.clone(),
                    self.deleted_values.clone(),
                ))),
            ) {
                *v
            } else {
                -1
            }
        }

        fn remove(&mut self, key: i32) {
            self.cache.lock_shard(&key).remove(&key);
        }
    }

    fn fn_deleter(
        keys: Arc<RwLock<Vec<i32>>>,
        values: Arc<RwLock<Vec<i32>>>,
    ) -> impl FnMut(&i32, &mut i32) {
        move |k, v| {
            keys.write().push(*k);
            values.write().push(*v);
        }
    }

    #[test]
    fn test_hit_and_miss() {
        let mut ct = ShardedCacheTester::new();
        assert_eq!(-1, ct.get_or_default(100));

        ct.insert(100, 101);
        assert_eq!(ct.get_or_default(100), 101);
        assert_eq!(ct.get_or_default(200), -1);
        assert_eq!(ct.get_or_default(300), -1);

        ct.insert(200, 201);
        assert_eq!(ct.get_or_default(100), 101);
        assert_eq!(ct.get_or_default(200), 201);
        assert_eq!(ct.get_or_default(300), -1);

        ct.insert(100, 102);
        assert_eq!(ct.get_or_default(100), 102);
        assert_eq!(ct.get_or_default(200), 201);
        assert_eq!(ct.get_or_default(300), -1);

        assert_eq!(ct.deleted_keys.read().len(), 1);
        assert_eq!(ct.deleted_keys.read()[0], 100);
        assert_eq!(ct.deleted_values.read()[0], 101);
    }

    #[test]
    fn test_remove() {
        let mut ct = ShardedCacheTester::new();
        ct.remove(200);
        assert_eq!(0, ct.deleted_keys.read().len());

        ct.insert(100, 101);
        ct.insert(200, 201);
        ct.remove(100);
        assert_eq!(ct.get_or_default(100), -1);
        assert_eq!(ct.get_or_default(200), 201);
        assert_eq!(ct.deleted_keys.read().len(), 1);
        assert_eq!(ct.deleted_keys.read()[0], 100);
        assert_eq!(ct.deleted_values.read()[0], 101);

        ct.remove(100);
        assert_eq!(ct.get_or_default(100), -1);
        assert_eq!(ct.get_or_default(200), 201);
        assert_eq!(ct.deleted_keys.read().len(), 1);
    }

    #[test]
    fn test_eviction_policy() {
        let mut ct = ShardedCacheTester::new();
        ct.insert(100, 101);
        ct.insert(200, 201);
        ct.insert(300, 301);

        assert_eq!(ct.cache.lock_shard(&300).get(&300), Some(&301));

        for i in 0..CACHE_SIZE + 100 {
            let i1 = i as i32;
            ct.insert(1000 + i1, 2000 + i1);
            assert_eq!(ct.get_or_default(1000 + i1), 2000 + i1);
            assert_eq!(ct.get_or_default(100), 101);
        }

        assert_eq!(ct.get_or_default(100), 101);
        assert_eq!(ct.get_or_default(200), -1);
        assert_eq!(ct.get_or_default(300), -1);
    }

    #[test]
    fn test_use_exceeds_cache_size() {
        let mut ct = ShardedCacheTester::new();
        let mut v = Vec::new();
        for i in 0..CACHE_SIZE + 100 {
            let i1 = i as i32;
            v.push(ct.insert_and_return_default(1000 + i1, 2000 + i1));
        }
        for (idx, val) in v.iter().enumerate().skip(100) {
            assert_eq!(ct.get_or_default(1000 + idx as i32), *val);
        }
    }

    #[test]
    fn test_heavy_entries() {
        let mut ct = ShardedCacheTester::new();
        const LIGHT: i32 = 1;
        const HEAVY: i32 = 10;
        let mut added = 0;
        let mut index = 0;
        while added < 2 * CACHE_SIZE {
            let weight = if (index & 1) == 1 { LIGHT } else { HEAVY };
            ct.insert_charge(index, 1000 + index, weight);
            added += weight as usize;
            index += 1;
        }

        let mut cached_weight = 0;
        for i in 0..index {
            let weight = if i & 1 == 1 { LIGHT } else { HEAVY };
            let r = ct.get_or_default(i);
            if r >= 0 {
                cached_weight += weight;
                assert_eq!(1000 + i, r);
            }
        }

        assert!(cached_weight <= CACHE_SIZE as i32 + CACHE_SIZE as i32 / 10);
    }

    #[test]
    fn test_prune() {
        let mut ct = ShardedCacheTester::new();
        ct.insert(1, 100);
        ct.insert(2, 200);
        assert_eq!(ct.cache.lock_shard(&1).get(&1), Some(&100));

        ct.cache.prune();
        assert_eq!(ct.get_or_default(1), -1);
        assert_eq!(ct.get_or_default(2), -1);
    }

    #[test]
    fn test_zero_size_cache() {
        let mut ct = ShardedCacheTester::new();
        ct.cache = ShardedCache::with_capacity(0);
        ct.insert(1, 100);
        assert_eq!(ct.get_or_default(1), -1);
    }
}
