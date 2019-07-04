# SPTP

Simple Payload Transmission Protocol

We assume protocol is handled via internal networking, thus UDP can be considered as a reliable transport.

The only problem is UDP packet size limitation, we solve this by chunk reassembling with unique message id.

## Packet Format

binary format, little-endian

* 1 byte, magic number, `0xAF`
* 1 byte, mode
    * bit 0, 1 = chunked message, 0 = simple message
    * bit 1, 1 = gzipped, 0 = plain
* (for chunked message only), 10 bytes, chunks info
    * 8 byte (uint64), unique message id, random generated
    * 1 byte, total chunk count
    * 1 byte, current chunk index
* n bytes, (segmented) payload

## Credit

Guo Y.K., MIT License
