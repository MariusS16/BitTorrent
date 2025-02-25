#include <algorithm>
#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

using namespace std;

#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 32
#define MAX_CHUNKS 100

struct ThreadParams {
  int num_files;
  vector<string> files;
  vector<int> num_chunks;
  vector<vector<string>> chunks;
  int num_files2;
  vector<string> files2;
  vector<vector<int>> clients;
};

ThreadParams trackerParams;
ThreadParams peerParams;
MPI_Status status;
int x;
int num_files2;

void *download_thread_func(void *arg) {
  int rank = *(int *)arg;
  x = 0;

  while (num_files2 < peerParams.num_files2) {
    for (int i = 0; i < peerParams.num_files2; i++, num_files2++) {
      int num_chunks_file;
      int num_clients;
      vector<string> chunks;
      MPI_Send(&x, 1, MPI_INT, TRACKER_RANK, 100, MPI_COMM_WORLD);
      MPI_Send(peerParams.files2[i].c_str(), MAX_FILENAME, MPI_CHAR, TRACKER_RANK, 100, MPI_COMM_WORLD);
      MPI_Recv(&num_chunks_file, 1, MPI_INT, TRACKER_RANK, 100, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      MPI_Recv(&num_clients, 1, MPI_INT, TRACKER_RANK, 100, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      vector<int> clients(num_clients);
      MPI_Recv(clients.data(), num_clients, MPI_INT, TRACKER_RANK, 100, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

      int clientIndex = 0;
      for (int j = 0; j < num_chunks_file; j++) {
        int client = clients[clientIndex];
        clientIndex = (clientIndex + 1) % num_clients;
        MPI_Send(&x, 1, MPI_INT, client, 200, MPI_COMM_WORLD);
        MPI_Send(&j, 1, MPI_INT, client, 200, MPI_COMM_WORLD);
        MPI_Send(peerParams.files2[i].c_str(), MAX_FILENAME, MPI_CHAR, client, 200, MPI_COMM_WORLD);
        char chunk[HASH_SIZE];
        MPI_Recv(chunk, HASH_SIZE, MPI_CHAR, client, 300, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        chunks.push_back(string(chunk));
      }

      string filename = "client" + to_string(rank) + "_" + peerParams.files2[i];
      FILE *file = fopen(filename.c_str(), "w");
      if (file == NULL) {
        printf("Eroare la deschiderea fisierului %s\n", filename.c_str());
        exit(-1);
      }
      for (const auto &chunk : chunks) {
        fprintf(file, "%s\n", chunk.c_str());
      }
      fclose(file);
    }
  }

  x = 1;
  MPI_Send(&x, 1, MPI_INT, TRACKER_RANK, 100, MPI_COMM_WORLD);
  return NULL;
}

void *upload_thread_func(void *arg) {
  while (true) {
    MPI_Recv(&x, 1, MPI_INT, MPI_ANY_SOURCE, 200, MPI_COMM_WORLD, &status);
    if (x) {
      break;
    }

    char filename[MAX_FILENAME];
    int chunk_index;
    MPI_Recv(&chunk_index, 1, MPI_INT, status.MPI_SOURCE, 200, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(filename, MAX_FILENAME, MPI_CHAR, status.MPI_SOURCE, 200, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    for (int i = 0; i < peerParams.num_files; i++) {
      if (peerParams.files[i] != string(filename)) {
        continue;
      }
      string chunk = peerParams.chunks[i][chunk_index];
      MPI_Send(chunk.c_str(), HASH_SIZE, MPI_CHAR, status.MPI_SOURCE, 300, MPI_COMM_WORLD);
    }
  }

  return NULL;
}

void tracker(int numtasks, int rank) {
  for (int i = 1; i < numtasks; i++) {
    MPI_Recv(&trackerParams.num_files, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    for (int j = 0; j < trackerParams.num_files; j++) {
      char filename[MAX_FILENAME];
      int num_chunks_file;
      MPI_Recv(filename, MAX_FILENAME, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      MPI_Recv(&num_chunks_file, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

      bool found = false;
      for (long unsigned int k = 0; k < trackerParams.files.size(); k++) {
        if (trackerParams.files[k] == string(filename)) {
          found = true;
          trackerParams.clients[k].push_back(i);
          break;
        }
      }

      vector<string> chunks_file;
      for (int k = 0; k < num_chunks_file; k++) {
        char chunk[HASH_SIZE];
        MPI_Recv(chunk, HASH_SIZE, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        chunks_file.push_back(chunk);
      }

      if (!found) {
        trackerParams.files.push_back(string(filename));
        trackerParams.clients.push_back(vector<int>(1, i));
        trackerParams.num_chunks.push_back(num_chunks_file);
        trackerParams.chunks.push_back(chunks_file);
      }
    }
  }

  for (int i = 1; i < numtasks; i++) {
    MPI_Send(NULL, 0, MPI_CHAR, i, 0, MPI_COMM_WORLD);
  }

  vector<bool> peers_done = vector<bool>(numtasks, false);
  while (true) {
    bool all_done = all_of(peers_done.begin() + 1, peers_done.begin() + numtasks, [](bool done) { return done; });
    if (all_done) {
      break;
    }

    MPI_Recv(&x, 1, MPI_INT, MPI_ANY_SOURCE, 100, MPI_COMM_WORLD, &status);

    if (!x) {
      char filename[MAX_FILENAME];
      MPI_Recv(filename, MAX_FILENAME, MPI_CHAR, status.MPI_SOURCE, 100, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

      int i = 0;
      for (long unsigned int j = 0; j < trackerParams.files.size(); j++) {
        if (trackerParams.files[j] == string(filename)) {
          i = j;
          break;
        }
      }

      int num_chunks_file = trackerParams.num_chunks[i];
      MPI_Send(&num_chunks_file, 1, MPI_INT, status.MPI_SOURCE, 100, MPI_COMM_WORLD);

      int num_clients = trackerParams.clients[i].size();
      MPI_Send(&num_clients, 1, MPI_INT, status.MPI_SOURCE, 100, MPI_COMM_WORLD);

      int clients2[num_clients];
      for (int j = 0; j < num_clients; j++) {
        clients2[j] = trackerParams.clients[i][j];
      }
      MPI_Send(clients2, num_clients, MPI_INT, status.MPI_SOURCE, 100, MPI_COMM_WORLD);
    } else {
      peers_done[status.MPI_SOURCE] = true;
    }
  }

  x = 1;
  for (int i = 1; i < numtasks; i++) {
    MPI_Send(&x, 1, MPI_INT, i, 200, MPI_COMM_WORLD);
  }
}

void peer(int numtasks, int rank) {
  string filename = "in" + to_string(rank) + ".txt";
  FILE *file = fopen(filename.c_str(), "r");
  if (file == NULL) {
    printf("Eroare la deschiderea fisierului %s\n", filename.c_str());
    exit(-1);
  }

  fscanf(file, "%d", &peerParams.num_files);
  MPI_Send(&peerParams.num_files, 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD);
  for (int i = 0; i < peerParams.num_files; i++) {
    char filename[MAX_FILENAME];
    int num_chunks;
    fscanf(file, "%s", filename);
    peerParams.files.push_back(filename);
    fscanf(file, "%d", &num_chunks);
    peerParams.num_chunks.push_back(num_chunks);
    MPI_Send(peerParams.files[i].c_str(), MAX_FILENAME, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
    MPI_Send(&peerParams.num_chunks[i], 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD);
    vector<string> chunks;
    for (int j = 0; j < peerParams.num_chunks[i]; j++) {
      char chunk[HASH_SIZE];
      fscanf(file, "%s", chunk);
      chunks.push_back(chunk);
      MPI_Send(chunks[j].c_str(), HASH_SIZE, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
    }
    peerParams.chunks.push_back(chunks);
  }

  fscanf(file, "%d", &peerParams.num_files2);
  for (int i = 0; i < peerParams.num_files2; i++) {
    char filename[MAX_FILENAME];
    fscanf(file, "%s", filename);
    peerParams.files2.push_back(filename);
  }

  fclose(file);
  MPI_Recv(NULL, 0, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

  pthread_t download_thread;
  pthread_t upload_thread;
  void *status;
  int r;

  r = pthread_create(&download_thread, NULL, download_thread_func, (void *)&rank);
  if (r) {
    printf("Eroare la crearea thread-ului de download\n");
    exit(-1);
  }

  r = pthread_create(&upload_thread, NULL, upload_thread_func, (void *)&rank);
  if (r) {
    printf("Eroare la crearea thread-ului de upload\n");
    exit(-1);
  }

  r = pthread_join(download_thread, &status);
  if (r) {
    printf("Eroare la asteptarea thread-ului de download\n");
    exit(-1);
  }

  r = pthread_join(upload_thread, &status);
  if (r) {
    printf("Eroare la asteptarea thread-ului de upload\n");
    exit(-1);
  }
}

int main(int argc, char *argv[]) {
  int numtasks, rank;

  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  if (provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "MPI nu are suport pentru multi-threading\n");
    exit(-1);
  }
  MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if (rank == TRACKER_RANK) {
    tracker(numtasks, rank);
  } else {
    peer(numtasks, rank);
  }

  MPI_Finalize();
}
