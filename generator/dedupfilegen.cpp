// dedupfilegen.cpp : This file contains the 'main' function. Program execution begins and ends there.
//
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <fstream>
using namespace std;

int main(int argc, char* argv[]) {


    if(argc < 3) {
                std::cerr << "Usage: " << argv[0] << " file_size_in_MB dedup_ratio output_file_mae" << std::endl;
        return 1;
    }

    char* data;
    long long file_size_MB = atoi(argv[1]); //size of file in MB
    long long dedup_ratio = atoi(argv[2]);



    long long MB_size = (long)1024 *(long)1024;
    long long file_size = file_size_MB * MB_size;

    long long buffer_size = file_size/dedup_ratio; // buffer size is at least 1MB
    if (buffer_size < MB_size) {
        buffer_size = MB_size;
    }

    ofstream wf(argv[3], ios::out | ios::binary);


    cout << argv[0] << " file size " << file_size_MB << " dedup ratio " << dedup_ratio <<" buffer_size=" << buffer_size << endl;


    srand((int)time(NULL));


    if (buffer_size > 1024 * 1024 * 1024) {
        cout << "buffer size is max" << endl;
        buffer_size = 1024 * 1024 * 1024; // maximum buffer size 1GB
    }

    data = new char[buffer_size];
    for (int i = 0; i < buffer_size; i++) {
        if ((i % 10000000) == 0) {
            cout << "generating random data " << i << " of " << buffer_size << endl;
        }
        data[i] = (char)rand();
    }



    if (!wf) {
        cout << "Cannot open file!" << endl;
        return 1;
    }
    long long curr_size = 0;
    while (curr_size < file_size) {
        long loc = rand() % (buffer_size - MB_size);
        wf.write(data+loc, MB_size);
        curr_size += MB_size;
    }

    for (int j = 0; j < 1; j++) {
    }
    wf.close();

}