//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logging_test.cpp
//
// Identification: test/logging/logging_test.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */
#include "logging/log_buffer.h"
#include "common/harness.h"
#include "gtest/gtest.h"
#include "network/peloton_server.h"
#include "network/postgres_protocol_handler.h"
#include "util/string_util.h"
#include "network/connection_handle_factory.h"
#include <iostream>
#include <cstdio>
#include <cassert>
#include <ctime>
#include <sys/time.h>
#include <unistd.h>


namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Tests
//===--------------------------------------------------------------------===//

class LoggingTests : public PelotonTest {};

// aa_profiling {start}
struct aa_TimePoint {
  struct timeval time_;
  char* point_name_;
};

#define aa_max_time_points_ 1000

// static const int aa_max_time_points_ = 50;
static int aa_total_count_ = 0;
static struct timeval aa_begin_time_;
static struct aa_TimePoint aa_time_points_[aa_max_time_points_]; // at most 50 time points.
static int aa_time_point_count_ = 0;
static bool aa_is_profiling_ = false;

void aa_BeginProfiling() {

  if (aa_is_profiling_ == true) {
    return;
  }

  ++aa_total_count_;

  gettimeofday(&aa_begin_time_, NULL);

  aa_time_point_count_ = 0;
  aa_is_profiling_ = true;
}

void aa_EndProfiling() {

  if (aa_is_profiling_ == false) {
    return;
  }
  struct timeval end_time;
  gettimeofday(&end_time, NULL);

  // if (aa_total_count_ % 500 == 0) {
  char buf[100];
  sprintf(buf, "/home/aarontian/peloton/benchmark_%ld.txt", aa_begin_time_.tv_sec);
  FILE *fp = fopen(buf, "a");



  fprintf(fp, "=================================\n");
  fprintf(fp, "txn count = %d\n", aa_total_count_);

  fprintf(fp, "driver_begin clock: %lf\n", aa_begin_time_.tv_sec * 1000.0 * 1000.0 + aa_begin_time_.tv_usec);
  int i;
  for (i = 0; i < aa_time_point_count_; ++i) {
    double diff = (aa_time_points_[i].time_.tv_sec - aa_begin_time_.tv_sec) * 1000.0 * 1000.0;
    diff += (aa_time_points_[i].time_.tv_usec - aa_begin_time_.tv_usec);

    fprintf(fp, "driver_point: %s, time: %lf us, clock: %lf\n", aa_time_points_[i].point_name_, diff, aa_time_points_[i].time_.tv_sec * 1000.0 * 1000.0 + aa_time_points_[i].time_.tv_usec);
  }

  double diff = (end_time.tv_sec - aa_begin_time_.tv_sec) * 1000.0 * 1000.0;
  diff += (end_time.tv_usec - aa_begin_time_.tv_usec);

  fprintf(fp, "driver_point: END, time: %lf us, clock: %lf\n", diff, end_time.tv_sec * 1000.0 * 1000.0 + end_time.tv_usec);

  fclose(fp);
  // }

  aa_time_point_count_ = 0;
  aa_is_profiling_ = false;

  printf("filename = %s\n", buf);
}

bool aa_IsProfiling() {
  return aa_is_profiling_;
}

void aa_InsertTimePoint(char* point_name) {
  if (aa_time_point_count_ < 0 || aa_time_point_count_ > aa_max_time_points_) {
    return;
  }
  struct aa_TimePoint *time_point = &(aa_time_points_[aa_time_point_count_]);

  gettimeofday(&(time_point->time_), NULL);



  time_point->point_name_ = point_name;

  ++aa_time_point_count_;
}
// aa_profiling {end}

void *LoggingTest(int port) {
  try {
    // forcing the factory to generate jdbc protocol handler
    pqxx::connection C(StringUtil::Format(
        "host=127.0.0.1 port=%d user=default_database sslmode=disable", port));
    LOG_INFO("[LoggingTest] Connected to %s", C.dbname());
    pqxx::work txn1(C);

    peloton::network::ConnectionHandle *conn =
        peloton::network::ConnectionHandleFactory::GetInstance().ConnectionHandleAt(
            peloton::network::PelotonServer::recent_connfd).get();

    //Check type of protocol handler
    network::PostgresProtocolHandler* handler =
        dynamic_cast<network::PostgresProtocolHandler*>(conn->GetProtocolHandler().get());

    EXPECT_NE(handler, nullptr);

    aa_BeginProfiling();
    // create table and insert some data
    txn1.exec("DROP TABLE IF EXISTS employee;");
    txn1.exec("CREATE TABLE employee(id INT, name VARCHAR(100));");
    txn1.commit();

    pqxx::work txn2(C);
    txn2.exec("INSERT INTO employee VALUES (1, 'Aaron Tian');");
    txn2.exec("INSERT INTO employee VALUES (2, 'Gandeevan Raghuraman');");
    txn2.exec("INSERT INTO employee VALUES (3, 'Anirudh Kanjani');");
    aa_InsertTimePoint((char *)"Insert Operations");
    for (int i = 4; i < 100000; i ++) {
      txn2.exec("INSERT INTO employee VALUES (" + std::to_string(i) + ", 'Anirudh Kanjani');");
    }
    aa_InsertTimePoint((char *)"Before txn2 commit");
    txn2.commit();
    aa_EndProfiling();

  } catch (const std::exception &e) {
    LOG_INFO("[LoggingTest] Exception occurred: %s", e.what());
    EXPECT_TRUE(false);
  }
  return NULL;
}

TEST_F(LoggingTests, LoggingTest) {
  peloton::PelotonInit::Initialize();
  LOG_INFO("Server initialized");
  peloton::network::PelotonServer server;

  int port = 15721;
  try {
    server.SetPort(port);
    server.SetupServer();
  } catch (peloton::ConnectionException &exception) {
    LOG_INFO("[LaunchServer] exception when launching server");
  }
  std::thread serverThread([&]() { server.ServerLoop(); });
  LoggingTest(port);
  server.Close();
  serverThread.join();
  peloton::PelotonInit::Shutdown();
  LOG_DEBUG("Peloton has shut down");

}
}
}