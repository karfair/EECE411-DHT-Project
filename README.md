# EECE411 DHT Project
group2

The branch attempts to do some of the optimization mentioned in the "Further Improvements"
section of the final report.

Currently, these features has been implemented (but have not been tested yet):
- Faster successor checking and minor bug fix
- Changed hashing algorithm to MD5
- No longer allocates 15k buffer for each incoming messages
- 'key' for CollectorThread in UDPServer has been changed from String to BigInteger (for performance)
