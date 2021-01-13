use super::*;
use crate::register::Register;
use std::borrow::Borrow;
use std::cmp::max;
use std::collections::HashMap;

/// Causal Length Map
///
/// A CRDT map based on an adaptation of the causal length set.
///
/// `Map` uses the tag for garbage collection of old removed members, and to
/// resolve conflicting values for the same key and causal length.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Map<K, V, Tag, CL>
where
    K: Key,
    V: Value + Hash,
    Tag: TagT,
    CL: CausalLength,
{
    map: HashMap<K, Register<V, Tag, CL>>,
}

impl<K, V, Tag, CL> Map<K, V, Tag, CL>
where
    K: Key,
    V: Value + Hash,
    Tag: TagT,
    CL: CausalLength,
{
    /// Create an empty `Map`
    pub fn new() -> Map<K, V, Tag, CL> {
        Map {
            map: HashMap::new(),
        }
    }

    /// Returns a reference to the value and tag corresponding to the key.
    pub fn get<Q>(&self, key: Q) -> Option<(&V, Tag)>
    where
        Q: Borrow<K>,
    {
        if let Some(e) = self.map.get(key.borrow()) {
            if e.length.is_odd() {
                return Some((&e.item, e.tag));
            }
        }
        None
    }

    /// Returns true if the map contains a value for the specified key.
    pub fn contains<Q>(&self, key: Q) -> bool
    where
        Q: Borrow<K>,
    {
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
                if oe.length.is_even() {
                    oe.length = oe.length + one;
                } else if oe.item != value {
                    // Special adaptation for a map: we add two to the causal length
                    // in cases where the key exists, but the value is not the same.
                    // This is equivalent to removing and re-adding the key.
                    oe.length = oe.length + one + one;
                }
                // always use the max value of tag
                oe.tag = max(oe.tag, tag);
                let r = oe.item.clone();
                oe.item = value;
                Some((r, oe.tag))
            }
            _ => {
                e.or_insert_with(|| Register::make(value, tag, one));
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
                oe.tag = max(oe.tag, tag);

                // {} if even(s(e))
                // { e |-> s(e) + 1 } if odd(s(e))
                if oe.length.is_odd() {
                    oe.length = oe.length + CL::one();
                    Some((oe.item.clone(), oe.tag))
                } else {
                    None
                }
            }
            _ => None,
        }
        // ignore attempts to remove items that aren't present...
    }

    /// An iterator visiting all key, value, tag tuples in arbitrary order.
    pub fn iter(&self) -> impl Iterator<Item = (K, V, Tag)> + '_ {
        self.map
            .iter()
            .filter(|(_k, v)| v.length.is_odd())
            .map(|(k, v)| (k.clone(), v.item.clone(), v.tag))
    }

    /// An iterator visiting all delta registers in arbitrary order.
    pub fn register_iter(&self) -> impl Iterator<Item = Register<(K, V), Tag, CL>> + '_ {
        self.map
            .iter()
            .map(|(k, v)| Register::make((k.clone(), v.item.clone()), v.tag, v.length))
    }

    /// Merge a delta [Register] into a map.
    ///
    /// Remove deltas with a tag value less than `min_tag` will be ignored.
    pub fn merge_register(&mut self, delta: Register<(K, V), Tag, CL>, min_tag: Tag) {
        let Register {
            item: (key, value),
            tag,
            length,
        } = delta;
        if delta.length.is_even() && delta.tag < min_tag {
            // ignore excessively old remove records
            return;
        }
        match self.map.entry(key) {
            Entry::Occupied(mut e) => {
                let e = e.get_mut();
                if length > e.length && tag > e.tag {
                    e.item = value;
                }
                // (s⊔s′)(e) = max(s(e),s′(e))
                e.tag = max(e.tag, tag);
                e.length = max(e.length, length);
            }
            Entry::Vacant(e) => {
                e.insert(Register::make(value, tag, length));
            }
        }
    }

    /// Merge two maps.
    ///
    /// Remove deltas with a tag value less than `min_tag` will be ignored.
    pub fn merge(&mut self, other: &Self, min_tag: Tag) {
        for delta in other.register_iter() {
            self.merge_register(delta, min_tag);
        }
    }

    /// Filter out old remove tombstone deltas from the map.
    ///
    /// Remove deltas with a tag value less than `min_tag` will be removed.
    pub fn retain(&mut self, min_tag: Tag) {
        self.map
            .retain(|_k, v| v.length.is_odd() || min_tag < v.tag);
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
        K: Key + Serialize,
        V: Value + Hash + Serialize,
        Tag: TagT + Serialize,
        CL: CausalLength + Serialize,
    {
        fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut seq = serializer.serialize_seq(Some(self.map.len()))?;
            for member in self.register_iter() {
                seq.serialize_element(&(member.item.0, member.item.1, member.tag, member.length))?;
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
        K: Key + Deserialize<'de>,
        V: Value + Hash + Deserialize<'de>,
        Tag: TagT + Deserialize<'de>,
        CL: CausalLength + Deserialize<'de>,
    {
        type Value = HashMap<K, Register<V, Tag, CL>>;

        fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a tuple of key, value, tag, and causal length")
        }

        fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut map: HashMap<K, Register<V, Tag, CL>> =
                HashMap::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(d) = seq.next_element::<(K, V, Tag, CL)>()? {
                map.insert(d.0, Register::make(d.1, d.2, d.3));
            }
            Ok(map)
        }
    }

    impl<'de, K, V, Tag, CL> Deserialize<'de> for Map<K, V, Tag, CL>
    where
        K: Key + Deserialize<'de>,
        V: Value + Hash + Deserialize<'de>,
        Tag: TagT + Deserialize<'de>,
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
use std::collections::hash_map::Entry;

#[cfg(test)]
mod tests {
    use super::*;
    use rand::seq::SliceRandom;

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
        assert_eq!(
            cls.map.get("foo"),
            Some(&Register {
                item: true,
                tag: later_time,
                length: 1
            })
        );
        assert_eq!(cls.contains("foo"), true);
        assert_eq!(cls.get("bar"), None);
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
        assert_eq!(
            cls.map.get("bar"),
            Some(&Register {
                item: true,
                tag: time_3,
                length: 3
            })
        );
        assert_eq!(
            cls.map.get("foo"),
            Some(&Register {
                item: true,
                tag: time_2,
                length: 2
            })
        );
        // check edges
        let values: Vec<(&str, bool, u32)> = cls.iter().collect();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], (("bar", true, time_3)));
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

        assert_eq!(cls2.map.len(), 2);
        assert_eq!(
            cls2.map.get(&"bar"),
            Some(&Register {
                item: 256,
                tag: time_3,
                length: 3
            })
        );
        assert_eq!(
            cls2.map.get(&"foo"),
            Some(&Register {
                item: 128,
                tag: time_2,
                length: 2
            })
        );

        let values: Vec<(&str, u32, u32)> = cls2.iter().collect();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], ("bar", 256, time_3));
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
        assert_eq!(
            cls.map.get(&"bar"),
            Some(&Register {
                item: 256,
                tag: time_2,
                length: 3
            })
        );
        assert_eq!(
            cls.map.get(&"foo"),
            Some(&Register {
                item: 128,
                tag: time_1,
                length: 2
            })
        );
        // check edges
        let values: Vec<(&str, u32, u32)> = cls.iter().collect();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], (("bar", 256, time_2)));
        // now clear old removes
        cls.retain(time_3);
        assert_eq!(cls.map.len(), 1);
        assert_eq!(
            cls.map.get(&"bar"),
            Some(&Register {
                item: 256,
                tag: time_2,
                length: 3
            })
        );
        // attempt to merge an out of date remove
        cls.merge_register(
            Register {
                item: ("bar", 512),
                tag: time_2,
                length: 2,
            },
            time_0,
        );
        assert_eq!(cls.map.len(), 1);
        assert_eq!(
            cls.map.get(&"bar"),
            Some(&Register {
                item: 256,
                tag: time_2,
                length: 3
            })
        );
    }

    #[test]
    fn test_overwrite() {
        let time_0 = 0;
        let time_1 = 1;
        let time_2 = 2;
        let mut cls: Map<&str, u32, u32, u16> = Map::new();

        cls.insert("bar", 256, time_0);
        cls.insert("bar", 256, time_1);
        // now try an overwrite
        cls.insert("bar", 512, time_2);
        assert_eq!(cls.map.len(), 1);
        assert_eq!(
            cls.map.get(&"bar"),
            Some(&Register {
                item: 512,
                tag: time_2,
                length: 3
            })
        );
    }

    #[cfg(feature = "serialization")]
    #[test]
    fn test_serialization() {
        let time_1 = 1;
        let time_2 = 2;
        let time_3 = 3;
        let mut m: Map<&str, bool, u32, u16> = Map::new();

        m.insert("foo", true, time_1);
        m.insert("bar", false, time_1);
        m.remove("foo", time_2);
        m.remove("bar", time_2);
        m.insert("bar", true, time_3);

        let data = serde_json::to_string(&m).unwrap_or("".to_owned());
        let cls2: Map<&str, bool, u32, u16> = serde_json::from_str(&data).unwrap();
        assert_eq!(m.map, cls2.map);
    }

    #[test]
    fn test_order_independence() {
        let mut m1: Map<&str, usize, u32, u16> = Map::new();
        let mut m2: Map<&str, usize, u32, u16> = Map::new();
        let mut v: Vec<Register<(&str, usize), u32, u16>> = vec![];

        for i in 0..1000 {
            v.push(Register {
                item: ("foo", i as usize),
                tag: i as u32,
                length: i as u16,
            });
        }

        // now randomize the updates
        v.shuffle(&mut rand::thread_rng());

        for r in v {
            m1.merge_register(r, 0);
        }
        assert_eq!(
            m1.map.get("foo"),
            Some(&Register {
                item: 999,
                tag: 999,
                length: 999
            })
        );

        let mut v: Vec<Register<(&str, usize), u32, u16>> = vec![];
        for i in 0..1000 {
            v.push(Register {
                item: ("foo", i as usize),
                tag: i as u32,
                length: i as u16,
            });
        }
        v.shuffle(&mut rand::thread_rng());
        for r in v {
            m2.merge_register(r, 0);
        }
        assert_eq!(m1, m2);
    }
}
