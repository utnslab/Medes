// Implementation of various class and static methods declared in
// data_structures.h

#include <curl/curl.h>
#include <sys/stat.h>

#include <boost/log/trivial.hpp>
#include <string>
#include <thread>
#include <vector>

#include "data_structures.h"

using std::string;
using std::vector;

int DockerCreateCheckpoint(int container_id, bool exit) {
  // BOOST_LOG_TRIVIAL(trace) << "Docker checkpoint: " << container_id;
  bool success = false;

  CURL *curl = curl_easy_init();
  long response_code;

  // Desired expected code is 201 for checkpoints
  int expected_code = 201;
  string url = "http://localhost/v1.40/containers/cont" +
               std::to_string(container_id) + "/checkpoints";
  if (curl) {
    // Register UNIX Socket
    curl_easy_setopt(curl, CURLOPT_UNIX_SOCKET_PATH, "/var/run/docker.sock");

    // Set Post Request
    curl_easy_setopt(curl, CURLOPT_POST, 1L);

    // Set the url for the post request
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());

    // Set the headers to accept json data
    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Accept: application/json");
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, "charset: utf-8");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    // Add the json object as CURLOPT_POSTFIELDS
    string exit_str = exit ? "true" : "false";
    string form_data = "{ \"CheckpointDir\" : \"\", \"CheckpointID\" : \"cp" +
                       std::to_string(container_id) +
                       "\", \"Exit\" : " + exit_str + "}";
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, form_data.c_str());

    // Set the timeout for the operation -- since no operation takes more than 5
    // seconds we are safe.
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
      BOOST_LOG_TRIVIAL(error)
          << "curl_easy_perform() failed: " << curl_easy_strerror(res);
    }

    // Check if response is as expected
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
    success = response_code == expected_code;

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    curl = NULL;
  }

  if (success) {
    return 0;
  }
  BOOST_LOG_TRIVIAL(error) << "Request failed error " << response_code;
  LaunchScript("test_container", std::to_string(container_id));
  return -1;
}

int DockerSingleOperation(int container_id, string op, int expected_code) {
  bool success = false;

  CURL *curl = curl_easy_init();
  long response_code;
  string url =
      "http://localhost/v1.40/containers/cont" + std::to_string(container_id);
  if (op != "remove" && op != "remove?force=true") {
    url += "/" + op;
  }
  if (op == "remove?force=true") {
    url += "?force=true";
  }

  if (curl) {
    // Register UNIX Socket
    curl_easy_setopt(curl, CURLOPT_UNIX_SOCKET_PATH, "/var/run/docker.sock");

    // Set Post Request
    if (op == "remove" || op == "remove?force=true") {
      curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    } else {
      curl_easy_setopt(curl, CURLOPT_POST, 1L);
    }

    // Set the url for the post request
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());

    // Set the timeout for the operation -- since no operation takes more than 5
    // seconds we are safe.
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
      BOOST_LOG_TRIVIAL(error) << "curl_easy_perform() failed: " << container_id
                               << " " << curl_easy_strerror(res);
    }

    // Check if response is as expected
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
    success = response_code == expected_code;

    curl_easy_cleanup(curl);
    curl = NULL;
  }

  if (success) {
    return 0;
  }
  BOOST_LOG_TRIVIAL(error) << "Request failed error " << container_id << " "
                           << op << " " << response_code;
  LaunchScript("test_container", std::to_string(container_id));
  return -1;
}

void LaunchScript(string name, string args) {
  // Redirect stderr to /dev/null so that stdout output can be verified.
  // string cmd = "./scripts/" + name + ".sh " + args + " 2> /dev/null";
  string cmd = "./scripts/" + name + ".sh " + args;
  int code = system(cmd.c_str());
  if (code != 0) {
    BOOST_LOG_TRIVIAL(error)
        << "Unsuccessful launch script with code " << std::to_string(code);
  }
}
