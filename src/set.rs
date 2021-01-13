use super::*;
use crate::register::Register;
use std::borrow::Borrow;
use std::cmp::max;
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Clone, Debug, PartialEq)]
struct SubRegister<Tag, CL>
where
    Tag: TagT,
    CL: CausalLength,
{
    tag: Tag,
    length: CL,
}

/// Causal Length Set
///
/// Set implements the set described in the paper, with the addition of a tag. Set only uses the
/// tag for garbage collection of old removed members.
#[derive(Clone, Debug, Default)]
pub struct Set<T, Tag, CL>
where
    T: Key,
    Tag: TagT,
    CL: CausalLength,
{
    // HashMap, because the "set" needs to allow mutating the tag and causal length.
    map: HashMap<T, SubRegister<Tag, CL>>,
}

impl<T, Tag, CL> Set<T, Tag, CL>
where
    T: Key,
    Tag: TagT,
    CL: CausalLength,
{
    /// Create a new empty `Set`
    pub fn new() -> Set<T, Tag, CL> {
        Set {
            map: HashMap::new(),
        }
    }

    /// Returns `None` if `member` is not present in the set. If present returns `Some(Tag)`
    pub fn get<Q>(&self, member: Q) -> Option<Tag>
    where
        Q: Borrow<T>,
    {
        if let Some(e) = self.map.get(member.borrow()).to_owned() {
            if e.length.is_odd() {
                return Some(e.tag);
            }
        }
        None
    }

    /// Returns true if the set contains a value.
    pub fn contains<Q>(&self, member: Q) -> bool
    where
        Q: Borrow<T>,
    {
        self.get(member).is_some()
    }

    /// Add a value to a set.
    pub fn add(&mut self, member: T, tag: Tag) {
        let one: CL = CL::one();
        let mut e = self
            .map
            .entry(member)
            .or_insert(SubRegister { tag, length: one });
        // s{e |-> s(e)+1} if even
        //s if odd s(e)
        if e.length.is_even() {
            e.length = e.length + one;
        }
        // always use the max value of tag
        e.tag = max(e.tag, tag);
    }

    /// Removes a value from the set.
    pub fn remove(&mut self, member: T, tag: Tag) {
        self.map.entry(member).and_modify(|e| {
            // {} if even(s(e))
            // { e |-> s(e) + 1 } if odd(s(e))
            if e.length.is_odd() {
                e.length = e.length + CL::one()
            }
            e.tag = max(e.tag, tag);
        });
        // ignore attempts to remove items that aren't present...
    }

    /// An iterator visiting all elements and tags in arbitrary order.
    pub fn iter(&self) -> impl Iterator<Item = (&T, Tag)> + '_ {
        self.map
            .iter()
            .filter(|(_k, v)| v.length.is_odd())
            .map(|(k, v)| (k, v.tag))
    }

    /// An iterator visiting all registers in arbitrary order.
    pub fn register_iter(&self) -> impl Iterator<Item = Register<T, Tag, CL>> + '_ {
        self.map.iter().map(|(k, v)| Register {
            item: k.clone(),
            tag: v.tag,
            length: v.length,
        })
    }

    /// Merge a delta [Register] into a set.
    ///
    /// Remove registers with a tag value less than `min_tag` will be ignored.
    pub fn merge_register(&mut self, delta: Register<T, Tag, CL>, min_tag: Tag) {
        if delta.length.is_even() && delta.tag < min_tag {
            // ignore excessively old remove records
            return;
        }
        let Register { item, tag, length } = delta;
        match self.map.entry(item) {
            Entry::Occupied(mut e) => {
                let e = e.get_mut();
                // (s⊔s′)(e) = max(s(e),s′(e))
                e.tag = max(e.tag, tag);
                e.length = max(e.length, length);
            }
            Entry::Vacant(e) => {
                e.insert(SubRegister { tag, length });
            }
        }
    }

    /// Merge two sets.
    ///
    /// Remove deltas with a tag value less than `min_tag` will be ignored.
    pub fn merge(&mut self, other: &Self, min_tag: Tag) {
        for delta in other.register_iter() {
            self.merge_register(delta, min_tag);
        }
    }

    /// Filter out old remove tombstone deltas from the set.
    ///
    /// Remove deltas with a tag value less than `min_tag` will be removed.
    pub fn retain(&mut self, min_tag: Tag) {
        self.map
            .retain(|_k, SubRegister { tag, length }| length.is_odd() || min_tag < *tag);
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

    impl<T, Tag, CL> Serialize for Set<T, Tag, CL>
    where
        T: Key + Serialize,
        Tag: TagT + Serialize,
        CL: CausalLength + Serialize,
    {
        fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut seq = serializer.serialize_seq(Some(self.map.len()))?;
            for member in self.register_iter() {
                seq.serialize_element(&(member.item, member.tag, member.length))?;
            }
            seq.end()
        }
    }

    struct DeltaVisitor<T, Tag, CL>(PhantomData<T>, PhantomData<Tag>, PhantomData<CL>);

    impl<'de, T, Tag, CL> Visitor<'de> for DeltaVisitor<T, Tag, CL>
    where
        T: Key + Deserialize<'de>,
        Tag: TagT + Deserialize<'de>,
        CL: CausalLength + Deserialize<'de>,
    {
        type Value = HashMap<T, SubRegister<Tag, CL>>;

        fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a tuple of key, value, tag, and causal length")
        }

        fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut map: HashMap<T, SubRegister<Tag, CL>> =
                HashMap::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(d) = seq.next_element::<(T, Tag, CL)>()? {
                map.insert(
                    d.0,
                    SubRegister {
                        tag: d.1,
                        length: d.2,
                    },
                );
            }
            Ok(map)
        }
    }

    impl<'de, T, Tag, CL> Deserialize<'de> for Set<T, Tag, CL>
    where
        T: Eq + Hash + Clone + Deserialize<'de>,
        Tag: TagT + Deserialize<'de>,
        CL: CausalLength + Deserialize<'de>,
    {
        fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let visitor = DeltaVisitor::<T, Tag, CL>(PhantomData, PhantomData, PhantomData);
            let map = deserializer.deserialize_seq(visitor)?;

            Ok(Set { map })
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
        let mut cls: Set<&str, u32, u16> = Set::new();

        cls.add("foo", later_time);
        cls.add("foo", later_time);
        cls.add("foo", later_time);
        assert_eq!(cls.map.len(), 1);
        assert_eq!(
            cls.map.get("foo"),
            Some(&SubRegister {
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
        let mut cls: Set<&str, u32, u16> = Set::new();

        cls.add("foo", time_1);
        cls.add("bar", time_1);
        cls.remove("foo", time_2);
        cls.remove("bar", time_2);
        cls.add("bar", time_3);
        // check map
        assert_eq!(cls.map.len(), 2);
        assert_eq!(
            cls.map.get(&"bar"),
            Some(&SubRegister {
                tag: time_3,
                length: 3
            })
        );
        assert_eq!(
            cls.map.get(&"foo"),
            Some(&SubRegister {
                tag: time_2,
                length: 2
            })
        );
        // check edges
        let values: Vec<(&&str, u32)> = cls.iter().collect();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], (&"bar", time_3));
    }

    #[test]
    fn test_merge() {
        let time_0 = 0;
        let time_1 = 1;
        let time_2 = 2;
        let time_3 = 3;
        let mut cls1: Set<&str, u32, u16> = Set::new();
        let mut cls2: Set<&str, u32, u16> = Set::new();

        cls1.add("foo", time_1);
        cls1.add("bar", time_1);
        cls2.merge(&cls1, time_0);
        cls2.remove("foo", time_2);
        cls1.remove("bar", time_2);
        cls1.remove("bar", time_2);
        cls2.merge(&cls1, time_0);
        cls2.add("bar", time_3);
        // check map
        assert_eq!(cls2.map.len(), 2);
        assert_eq!(
            cls2.map.get(&"bar"),
            Some(&SubRegister {
                tag: time_3,
                length: 3
            })
        );
        assert_eq!(
            cls2.map.get(&"foo"),
            Some(&SubRegister {
                tag: time_2,
                length: 2
            })
        );
        // check edges
        let values: Vec<(&&str, u32)> = cls2.iter().collect();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], (&"bar", time_3));
    }

    #[test]
    fn test_retain() {
        let time_0 = 0;
        let time_1 = 1;
        let time_2 = 2;
        let time_3 = 3;
        let mut cls: Set<&str, u32, u16> = Set::new();

        cls.add("foo", time_0);
        cls.add("bar", time_0);
        cls.remove("foo", time_1);
        cls.remove("bar", time_1);
        cls.add("bar", time_2);
        // check map
        assert_eq!(cls.map.len(), 2);
        assert_eq!(
            cls.map.get(&"bar"),
            Some(&SubRegister {
                tag: time_2,
                length: 3
            })
        );
        assert_eq!(
            cls.map.get(&"foo"),
            Some(&SubRegister {
                tag: time_1,
                length: 2
            })
        );
        // check edges
        let values: Vec<(&&str, u32)> = cls.iter().collect();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], (&"bar", time_2));
        // now clear old removes
        cls.retain(time_3);
        assert_eq!(cls.map.len(), 1);
        assert_eq!(
            cls.map.get(&"bar"),
            Some(&SubRegister {
                tag: time_2,
                length: 3
            })
        );
        // attempt to merge an out of date remove
        cls.merge_register(
            Register {
                item: &"bar",
                tag: time_2,
                length: 2,
            },
            time_0,
        );
        assert_eq!(cls.map.len(), 1);
        assert_eq!(
            cls.map.get(&"bar"),
            Some(&SubRegister {
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
        let mut cls: Set<&str, u32, u16> = Set::new();

        cls.add("foo", time_1);
        cls.add("bar", time_1);
        cls.remove("foo", time_2);
        cls.remove("bar", time_2);
        cls.add("bar", time_3);

        let data = serde_json::to_string(&cls).unwrap_or("".to_owned());
        let cls2: Set<&str, u32, u16> = serde_json::from_str(&data).unwrap();
        assert_eq!(cls.map, cls2.map);
    }

    #[test]
    fn test_order_independence() {
        let mut m: Set<&str, u32, u16> = Set::new();
        let mut v: Vec<Register<&str, u32, u16>> = vec![];

        for i in 0..1000 {
            v.push(Register {
                item: "foo",
                tag: i as u32,
                length: i as u16,
            });
        }

        // now randomize the updates
        v.shuffle(&mut rand::thread_rng());

        for r in v {
            m.merge_register(r, 0);
        }
        assert_eq!(
            m.map.get("foo"),
            Some(&SubRegister {
                tag: 999,
                length: 999
            })
        );
    }
}
