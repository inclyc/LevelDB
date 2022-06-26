use super::memory_buffer::MemoryBuffer;
#[test]
fn test_write_query() {
    let mut buffer = MemoryBuffer::new(64, None);
    let test_pk = "some_pk";
    for i in 1..8 {
        buffer.write(i, test_pk, i);
    }
    assert_eq!(buffer.query(test_pk, 1).len(), 3); // 2 4 6
    assert_eq!(buffer.query(test_pk, 2).len(), 1); // 4
    assert_eq!(buffer.query(test_pk, 0).len(), 7);
    for i in 1..8 {
        assert_eq!(buffer.query("some_other_pk", i).len(), 0);
    }
}
#[test]
fn test_many_write() {
    let mut buffer = MemoryBuffer::new(64, None);
    let test_pk = "some_pk";
    for i in 1..1000000 {
        buffer.write(i, test_pk, i); // < 1ns
    } // 0.93s
    assert_eq!(buffer.query(test_pk, 5).len(), 31249);
}
