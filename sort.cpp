#include<iostream>
#include<bits/stdc++.h>
#include<sys/time.h>
#include<pthread.h>
#include<stdio.h>
#include<stdlib.h>

using namespace std;

struct pthreadArgs
{
        string filename;
};

struct heapArgs
{
        string line;
        int file_no;
};

struct heapComparator
{
        bool operator()(heapArgs const& a, heapArgs const&b)
        {
                return a.line > b.line;
        }

};
void swap();
int partition(vector<string> &vec, int low, int high);
void quickSort(vector<string> &vec, int low, int high);
void noPartition(string filename);
void withPartition(string filename, long long int file_size);
void multithreading(long long int file_size, int file_no);
void *sortMultithreading(void *arg);
void mergingFiles(long long int file_size);

int main(int argc, char *argv[])
{

        string filename = argv[1];
        long long int file_size = atol(argv[2]);

        long long int partition_size = 4000000000;
        struct timeval start, end;
        double time_taken;
        int no_of_times = file_size / partition_size;

        if(file_size > partition_size)
        {
                cout<<"File cannot fit in RAM\n";
                cout<<"Partition Required\n";

                gettimeofday(&start, NULL);

                struct timeval partitionStart, partitionEnd;
                struct timeval multiSortingStart, multiSortingEnd;
                struct timeval mergingStart, mergingEnd;

                gettimeofday(&partitionStart, NULL);
                withPartition(filename, file_size);
                gettimeofday(&partitionEnd, NULL);

                time_taken = (((long) partitionEnd.tv_sec - (long) partitionStart.tv_sec) * 1000000 + (partitionEnd.tv_usec - partitionStart.tv_usec)) / 1000000;
        printf("Time taken to partition the file is %lf secs\n", time_taken);

                gettimeofday(&multiSortingStart, NULL);
                int file_no = 0;
                for(int i = 0; i < no_of_times; i++)
                {
                        multithreading(partition_size, file_no);
                        file_no += 8;
                }
                gettimeofday(&multiSortingEnd, NULL);

                time_taken = (((long) multiSortingEnd.tv_sec - (long) multiSortingStart.tv_sec) * 1000000 + (multiSortingEnd.tv_usec - multiSortingStart.tv_usec)) / 1000000;
        printf("Time taken to only sort the file is %lf secs\n", time_taken);

                gettimeofday(&mergingStart, NULL);
                mergingFiles(file_size);
                gettimeofday(&mergingEnd, NULL);

                time_taken = (((long) mergingEnd.tv_sec - (long) mergingStart.tv_sec) * 1000000 + (mergingEnd.tv_usec - mergingStart.tv_usec)) / 1000000;
        printf("Time taken to mergs the files is %lf secs\n", time_taken);

                gettimeofday(&end, NULL);

                time_taken = (((long) end.tv_sec - (long) start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec)) / 1000000;

        printf("Time taken for overall sorting the file is %lf secs\n", time_taken);
        }

        else
        {
                cout<<"No Partition Required"<<endl;
                cout<<"File can fit in RAM"<<endl;

                gettimeofday(&start, NULL);
                noPartition(filename);
                gettimeofday(&end, NULL);

                time_taken = (((long) end.tv_sec - (long) start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec)) / 1000000;

                printf("Time taken to sort the file is %lf secs\n", time_taken);
        }
}

void mergingFiles(long long int file_size)
{

        long long int single_file_size = 500000000;
        int no_of_files = file_size / single_file_size;

        priority_queue <heapArgs, vector<heapArgs>, heapComparator> pq;

        fstream read_objs[no_of_files];
        char filenames[15];
        string readline;

        for(int i = 0; i < no_of_files; i++)
        {

                snprintf(filenames, 15, "file%d.txt", i + 1);
                read_objs[i].open(filenames);

                getline(read_objs[i], readline);

                heapArgs temp;
                temp.line = readline;
                temp.file_no = i;

                pq.push(temp);

        }

        fstream write_obj;
        write_obj.open("final_sort_file.txt", ios::out);

        while(!pq.empty())
        {
                heapArgs temp;
                int fileNo;

                temp = pq.top();
                pq.pop();

                write_obj << temp.line << endl;
                fileNo = temp.file_no;

                if(!read_objs[fileNo].eof())
                {
                        getline(read_objs[fileNo], readline);
                        heapArgs nextLine;

                        nextLine.line = readline;
                        nextLine.file_no = fileNo;


                        pq.push(nextLine);
                }
        }

        for(int i = 0;  i < no_of_files; i++)
        {
                read_objs[i].close();
        }

}
void multithreading(long long int file_size, int file_no)
{

        long long int single_file_size = 500000000;
        int no_of_threads = file_size / single_file_size;

        pthread_t threadIds[no_of_threads];

        struct pthreadArgs arg[no_of_threads];

        for(int i = 0; i < no_of_threads; i++)
        {
                char filenames[15];

                snprintf(filenames, 15, "file%d.txt", i+file_no+1);
                arg[i].filename = filenames;
        }

        for(int i = 0; i < no_of_threads; i++)
        {
                pthread_create(&threadIds[i], NULL, &sortMultithreading, &arg[i]);
        }

        for(int i = 0; i < no_of_threads; i++)
        {
                        pthread_join(threadIds[i], NULL);
        }

//      pthread_exit(NULL);
}

void *sortMultithreading(void *arg)
{

        struct pthreadArgs *param = (struct pthreadArgs*) arg;

        //printf("printing %s", param -> filename);
        cout<<param->filename<<endl;

        fstream obj;
        string readline;

        obj.open(param->filename, ios::in);
        vector<string> vec;

        while(obj)
        {
                getline(obj, readline);

                if(readline == "-1")
                        break;

                vec.push_back(readline);
        }

        obj.close();
        /*
        cout<<"printing vector value"<<endl;
        for(int i = 0; i < vec.size(); i++)
        {
                cout<<vec[i]<<endl;
        }
        cout<<"after sorting"<<endl;
        */
        cout<<"Starting sorting"<<endl;
        //sort(vec.begin(), vec.end());
        quickSort(vec, 0, vec.size() - 1);
        cout<<"Sorting finished"<<endl;
/*
        for(int i = 0; i < vec.size(); i++)
        {

                cout<<vec[i]<<endl;
        }
*/
        obj.open(param -> filename, ios::out);
        cout<<"Starting to write"<<endl;
        for(int i = 0; i < vec.size(); i++)
        {
                obj << vec[i] << endl;
        }

        vec.clear();
        obj.close();
}


void withPartition(string filename, long long int file_size)
{

        fstream obj;
        string readline;

        obj.open(filename, ios::in);

        long long int single_file_size = 500000000;
        int no_of_files = file_size/single_file_size;
        long long int no_of_lines = single_file_size/100;

        cout<<"No of files and lines per file "<< no_of_files<<" "<<no_of_lines<<endl;
        for(int i = 0; i < no_of_files; i++)
        {
                char filenames[15];
                snprintf(filenames, 15, "file%d.txt", i+1);
                ofstream of(filenames);

            for(int j = 0; j < no_of_lines; j++)
                {

                        getline(obj, readline);

                        if(readline == "-1")
                                break;
                //      cout<<readline<<endl;
                        of << readline << endl;
                }
                of.close();
        }
        obj.close();
}

void noPartition(string filename)
{

        fstream obj;
        string readline;

        obj.open(filename, ios::in);
        vector<string> vec;

        while(obj)
        {
                getline(obj, readline);

                if(readline == "-1")
                        break;

                vec.push_back(readline);
        }

        obj.close();
        /*
        cout<<"printing vector value"<<endl;
        for(int i = 0; i < vec.size(); i++)
        {
                cout<<vec[i]<<endl;
        }
        cout<<"after sorting"<<endl;
        */
        cout<<"Starting sorting"<<endl;
        //sort(vec.begin(), vec.end());
        quickSort(vec, 0, vec.size() - 1);
        cout<<"Sorting finished"<<endl;
/*
        for(int i = 0; i < vec.size(); i++)
        {

                cout<<vec[i]<<endl;
        }
*/
        obj.open(filename, ios::out);
        cout<<"Starting to write"<<endl;
        for(int i = 0; i < vec.size(); i++)
        {
                obj << vec[i] << endl;
        }

        obj.close();

}
int partition (vector<string> &vec, int low, int high)
{
    string pivot = vec[high]; // pivot
    int i = (low - 1); // Index of smaller element
    string temp;

    for (int j = low; j <= high - 1; j++)
    {
        if ((vec[j].compare(pivot)) < 0)//vec[j] < pivot)
        {
            i++; // increment index of smaller element
            temp = vec[j];
            vec[j] = vec[i];
            vec[i] = temp;
        }
    }
    temp = vec[high];
    vec[high] = vec[i + 1];
    vec[i + 1] = temp;
    return (i + 1);
}

void quickSort(vector<string> &vec, int low, int high)
{
    if (low < high)
    {
        int pi = partition(vec, low, high);

        // Separately sort elements before
        // partition and after partition
        quickSort(vec, low, pi - 1);
        quickSort(vec, pi + 1, high);
    }
}