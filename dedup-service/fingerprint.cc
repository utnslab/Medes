// Includes all functions used for fingerprint calculation.

#include <openssl/sha.h>

#include <algorithm>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

#include "data_structures.h"

#define CHUNK_SIZE 64
#define SKIP_SIZE 16

using std::string;
using std::stringstream;
using std::vector;

bool IsCharArrayZero(char array[], std::ptrdiff_t length) {
  return std::all_of(array, array + length, [](char x) { return x == '\0'; });
}

string ComputeChunkHash(char *chunk, int chunk_size) {
  const unsigned char *char_chunk =
      reinterpret_cast<const unsigned char *>(chunk);
  unsigned char char_hash[SHA_DIGEST_LENGTH];  // == 20
  SHA1(char_chunk, chunk_size, char_hash);

  // Convert into a string of ASCII characters
  stringstream ss;
  for (int i = 0; i < SHA_DIGEST_LENGTH; i++) {
    ss << std::hex << std::setw(2) << std::setfill('0') << (int)char_hash[i];
  }

  return ss.str();
}

// Use `num_chunks' chunks on the page at fixed equi-distant offsets
vector<string> FixedOffsetFingerprint(char *page, int num_chunks) {
  int ichunk = 0;
  vector<string> fingerprints;

  while (ichunk < num_chunks) {
    int chunk_offset = ichunk * PAGE_SIZE / num_chunks;
    string fingerprint = ComputeChunkHash(&page[chunk_offset], CHUNK_SIZE);

    fingerprints.push_back(fingerprint);
    ichunk += 1;
  }

  return fingerprints;
}

// Use a non-null chunk per segment, with the segment boundaries being defined
// by the equidistant offsets.
vector<string> NonNullFingerprint(char *page, int num_chunks) {
  // Null Fingerprint
  string null_fp = "c8d7d0ef0eedfa82d2ea1aa592845b9a6d4b02b7";
  vector<string> fingerprints;

  int ichunk = 0;
  bool found = false;
  while (ichunk < num_chunks) {
    int segment_size = PAGE_SIZE / num_chunks;
    int segment_offset = ichunk * segment_size;

    // Add hashes to the PageStruct
    for (int chunk_offset = 0; chunk_offset < segment_size;
         chunk_offset += SKIP_SIZE) {
      if (IsCharArrayZero(&page[segment_offset + chunk_offset], CHUNK_SIZE))
        continue;

      string fingerprint =
          ComputeChunkHash(&page[segment_offset + chunk_offset], CHUNK_SIZE);
      fingerprints.push_back(fingerprint);
      found = true;
      break;
    }

    ichunk += 1;
  }

  if (!found) {
    // No non-null fingerprint was found. Use the default one as the single one.
    fingerprints.push_back(null_fp);
  }

  return fingerprints;
}

// Take a fingerprint every few bytes and use fingerprints that have a fixed
// pattern in the last digits. If no relevant fingerprint found -- use the null
// fingerprint.
vector<string> ValueSampledFingerprint(char *page, int num_chunks) {
  string null_fp = "c8d7d0ef0eedfa82d2ea1aa592845b9a6d4b02b7";
  vector<string> fingerprints;

  for (int chunk_offset = 0; chunk_offset < PAGE_SIZE;
       chunk_offset += SKIP_SIZE) {
    if (IsCharArrayZero(&page[chunk_offset], CHUNK_SIZE)) continue;

    string fingerprint = ComputeChunkHash(&page[chunk_offset], CHUNK_SIZE);

    // Use fingerprints that have last bit zero
    int last_four = fingerprint.at(fingerprint.length() - 1) & 0x0F;
    if (last_four % 2 == 0) {
      fingerprints.push_back(fingerprint);
    }

    if (fingerprints.size() == num_chunks) {
      break;
    }
  }

  if (fingerprints.size() == 0) {
    fingerprints.push_back(null_fp);
  }

  return fingerprints;
}
