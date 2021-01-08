use super::*;
use std::cmp::{max, Ord};
use std::collections::HashMap;
use std::hash::Hash;

/// Causal Length Map
///
/// A CRDT map based on an adaptation of the causal length set.
///
/// The Map uses the tag for garbage collection of old removed members, and to
/// resolve conflicting values for the same key and causal length.
#[derive(Clone, Debug, Default)]
pub struct Map<K, V, Tag, CL>
where
    K: Eq + Hash + Clone,
    V: Clone + Eq,
    Tag: Ord + Copy,
    CL: CausalLength,
{
    map: HashMap<K, (V, Tag, CL)>,
}

impl<K, V, Tag, CL> Map<K, V, Tag, CL>
where
    K: Eq + Hash + Clone,
    V: Clone + Eq,
    Tag: Ord + Copy,
    CL: CausalLength,
{
    /// Create an empty `Map`
    pub fn new() -> Map<K, V, Tag, CL> {
        Map {
            map: HashMap::new(),
        }
    }

    /// Returns a reference to the value and tag corresponding to the key.
    pub fn get(&self, key: &K) -> Option<(&V, Tag)> {
        if let Some(e) = self.map.get(key).to_owned() {
            if e.2.is_odd() {
                return Some((&e.0, e.1));
            }
        }
        None
    }

    /// Returns true if the map contains a value for the specified key.
    pub fn contains(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    /// Inserts a key, value, and tag into the map.
    ///
    /// If the map did not have this key present, [`None`] is returned.
    ///
    /// If the map did have this key present, the value is updated, and the old
    /// value is returned, along with the old tag.
    pub fn insert(&mut self, key: K, value: V, tag: Tag) -> Option<(V, Tag)> {
        let one: CL = CL::one();
        let e = self.map.entry(key);
        match e {
            std::collections::hash_map::Entry::Occupied(mut oe) => {
                let oe = oe.get_mut();
                // s{e |-> s(e)+1} if even
                //s if odd s(e)
                if oe.2.is_even() {
                    oe.2 = oe.2 + one;
                } else if oe.0 != value {
                    // Special adaptation for a map: we add two to the causal length
                    // in cases where the key exists, but the value is not the same.
                    // This is equivalent to removing and re-adding the key.
                    oe.2 = oe.2 + one + one;
                }
                // always use the max value of tag
                oe.1 = max(oe.1, tag);
                let r = oe.0.clone();
                oe.0 = value;
                Some((r, oe.1))
            }
            _ => {
                e.or_insert((value, tag, one));
                None
            }
        }
    }

    /// Remove a key from the map, returning the stored value and tag if
    /// the key was in the map.
    pub fn remove(&mut self, key: K, tag: Tag) -> Option<(V, Tag)> {
        let e = self.map.entry(key);
        match e {
            std::collections::hash_map::Entry::Occupied(mut oe) => {
                let oe = oe.get_mut();
                oe.1 = max(oe.1, tag);

                // {} if even(s(e))
                // { e |-> s(e) + 1 } if odd(s(e))
                if oe.2.is_odd() {
                    oe.2 = oe.2 + CL::one();
                    Some((oe.0.clone(), oe.1))
                } else {
                    None
                }
            }
            _ => None,
        }
        // ignore attempts to remove items that aren't present...
    }

    /// An iterator visiting all key, value, tag tuples in arbitrary order.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V, Tag)> + '_ {
        self.map
            .iter()
            .filter(|(_k, v)| v.2.is_odd())
            .map(|(k, v)| (k, &v.0, v.1))
    }

    /// An iterator visiting all delta tuples in arbitrary order.
    pub fn delta_iter(&self) -> impl Iterator<Item = (&K, &V, Tag, CL)> + '_ {
        self.map.iter().map(|(k, v)| (k, &v.0, v.1, v.2))
    }

    /// Merge a delta tuple into a map.
    ///
    /// Remove deltas with a tag value less than `min_tag` will be ignored.
    pub fn merge_delta(&mut self, delta: (&K, &V, Tag, CL), min_tag: Tag) {
        let (key, value, tag, length) = delta;

        if length.is_even() && tag < min_tag {
            // ignore excessively old remove records
            return;
        }
        self.map
            .entry(key.clone())
            .and_modify(|e| {
                if length > e.2 && tag > e.1 {
                    e.0 = value.clone();
                }
                // (s⊔s′)(e) = max(s(e),s′(e))
                e.1 = max(e.1, tag);
                e.2 = max(e.2, length);
            })
            .or_insert((value.clone(), tag, length));
    }

    /// Merge two maps.
    ///
    /// Remove deltas with a tag value less than `min_tag` will be ignored.
    pub fn merge(&mut self, other: &Self, min_tag: Tag) {
        for delta in other.delta_iter() {
            self.merge_delta(delta, min_tag);
        }
    }

    /// Filter out old remove tombstone deltas from the map.
    ///
    /// Remove deltas with a tag value less than `min_tag` will be removed.
    pub fn retain(&mut self, min_tag: Tag) {
        self.map
            .retain(|_k, (_v, tag, length)| length.is_odd() || min_tag < *tag);
    }
}

#[cfg(feature = "serialization")]
mod serialization {
    use super::*;
    use serde::de::{SeqAccess, Visitor};
    use serde::export::Formatter;
    use serde::ser::SerializeSeq;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::marker::PhantomData;

    impl<K, V, Tag, CL> Serialize for Map<K, V, Tag, CL>
    where
        K: Eq + Hash + Clone + Serialize,
        V: Clone + Eq + Serialize,
        Tag: Ord + Copy + Serialize,
        CL: CausalLength + Serialize,
    {
        fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut seq = serializer.serialize_seq(Some(self.map.len()))?;
            for member in self.delta_iter() {
                seq.serialize_element(&member)?;
            }
            seq.end()
        }
    }

    struct DeltaVisitor<K, V, Tag, CL>(
        PhantomData<K>,
        PhantomData<V>,
        PhantomData<Tag>,
        PhantomData<CL>,
    );

    impl<'de, K, V, Tag, CL> Visitor<'de> for DeltaVisitor<K, V, Tag, CL>
    where
        K: Eq + Hash + Clone + Deserialize<'de>,
        V: Clone + Eq + Deserialize<'de>,
        Tag: Ord + Copy + Deserialize<'de>,
        CL: CausalLength + Deserialize<'de>,
    {
        type Value = HashMap<K, (V, Tag, CL)>;

        fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an integer between 0 and 2^64")
        }

        fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut map: HashMap<K, (V, Tag, CL)> =
                HashMap::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(d) = seq.next_element::<(K, V, Tag, CL)>()? {
                map.insert(d.0, (d.1, d.2, d.3));
            }
            Ok(map)
        }
    }

    impl<'de, K, V, Tag, CL> Deserialize<'de> for Map<K, V, Tag, CL>
    where
        K: Eq + Hash + Clone + Deserialize<'de>,
        V: Clone + Eq + Deserialize<'de>,
        Tag: Ord + Copy + Deserialize<'de>,
        CL: CausalLength + Deserialize<'de>,
    {
        fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let visitor =
                DeltaVisitor::<K, V, Tag, CL>(PhantomData, PhantomData, PhantomData, PhantomData);
            let map = deserializer.deserialize_seq(visitor)?;

            Ok(Map { map })
        }
    }
}

#[cfg(feature = "serialization")]
pub use serialization::*;

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "serialization")]
    use serde_json;

    #[test]
    fn test_add() {
        let later_time = 1;
        let mut cls: Map<&str, bool, u16, u16> = Map::new();

        cls.insert("foo", true, later_time);
        cls.insert("foo", true, later_time);
        cls.insert("foo", true, later_time);
        assert_eq!(cls.map.len(), 1);
        assert_eq!(cls.map.get("foo"), Some(&(true, later_time, 1)));
        assert_eq!(cls.contains(&"foo"), true);
        assert_eq!(cls.get(&"bar"), None);
    }

    #[test]
    fn test_remove() {
        let time_1 = 1;
        let time_2 = 2;
        let time_3 = 3;
        let mut cls: Map<&str, bool, u32, u16> = Map::new();

        cls.insert("foo", true, time_1);
        cls.insert("bar", false, time_1);
        cls.remove("foo", time_2);
        cls.remove("bar", time_2);
        cls.insert("bar", true, time_3);
        // check map
        assert_eq!(cls.map.len(), 2);
        assert_eq!(cls.map.get("bar"), Some(&(true, time_3, 3)));
        assert_eq!(cls.map.get("foo"), Some(&(true, time_2, 2)));
        // check edges
        let values: Vec<(&&str, &bool, u32)> = cls.iter().collect();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], ((&"bar", &true, time_3)));
    }

    #[test]
    fn test_merge() {
        let time_0 = 0;
        let time_1 = 1;
        let time_2 = 2;
        let time_3 = 3;
        let mut cls1: Map<&str, u32, u32, u16> = Map::new();
        let mut cls2: Map<&str, u32, u32, u16> = Map::new();

        cls1.insert("foo", 128, time_1);
        cls1.insert("bar", 256, time_1);
        cls2.merge(&cls1, time_0);
        cls2.insert("foo", 128, time_2);
        cls1.remove("foo", time_2);
        cls1.remove("bar", time_2);
        cls2.merge(&cls1, time_0);
        cls2.insert("bar", 256, time_3);
        // check map
        assert_eq!(cls2.map.len(), 2);
        assert_eq!(cls2.map.get(&"bar"), Some(&(256, time_3, 3)));
        assert_eq!(cls2.map.get(&"foo"), Some(&(128, time_2, 2)));
        // check edges
        let values: Vec<(&&str, &u32, u32)> = cls2.iter().collect();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], (&"bar", &256, time_3));
    }

    #[test]
    fn test_retain() {
        let time_0 = 0;
        let time_1 = 1;
        let time_2 = 2;
        let time_3 = 3;
        let mut cls: Map<&str, u32, u32, u16> = Map::new();

        cls.insert("foo", 128, time_0);
        cls.insert("bar", 256, time_0);
        cls.remove("foo", time_1);
        cls.remove("bar", time_1);
        cls.insert("bar", 256, time_2);
        // check map
        assert_eq!(cls.map.len(), 2);
        assert_eq!(cls.map.get(&"bar"), Some(&(256, time_2, 3)));
        assert_eq!(cls.map.get(&"foo"), Some(&(128, time_1, 2)));
        // check edges
        let values: Vec<(&&str, &u32, u32)> = cls.iter().collect();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], ((&"bar", &256, time_2)));
        // now clear old removes
        cls.retain(time_3);
        assert_eq!(cls.map.len(), 1);
        assert_eq!(cls.map.get(&"bar"), Some(&(256, time_2, 3)));
        // attempt to merge an out of date remove
        cls.merge_delta((&"bar", &512, time_2, 2), time_0);
        assert_eq!(cls.map.len(), 1);
        assert_eq!(cls.map.get(&"bar"), Some(&(256, time_2, 3)));
    }

    #[test]
    fn test_overwrite() {
        let time_0 = 0;
        let time_1 = 1;
        let time_2 = 2;
        let mut cls: Map<&str, u32, u32, u16> = Map::new();

        cls.insert("foo", 128, time_0);
        cls.insert("bar", 256, time_0);
        cls.insert("bar", 256, time_1);
        println!("{:#?}", cls.map);
        cls.insert("foo", 512, time_2);
        println!("{:#?}", cls.map);

        // TODO: finish writing this test...
    }

    #[cfg(feature = "serialization")]
    #[test]
    fn test_serialization() {
        let time_1 = 1;
        let time_2 = 2;
        let time_3 = 3;
        let mut cls: Map<&str, bool, u32, u16> = Map::new();

        cls.insert("foo", true, time_1);
        cls.insert("bar", false, time_1);
        cls.remove("foo", time_2);
        cls.remove("bar", time_2);
        cls.insert("bar", true, time_3);

        let data = serde_json::to_string(&cls).unwrap_or("".to_owned());
        println!("{:#?}", data);
        let cls2: Map<&str, bool, u32, u16> = serde_json::from_str(&data).unwrap();
        assert_eq!(cls.map, cls2.map);
    }
}
