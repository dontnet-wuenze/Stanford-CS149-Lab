#ifndef _TASKSYS_H
#define _TASKSYS_H

#include <cstddef>
#include <iostream>
#include "itasksys.h"
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <map>

class DependencyGraph;

class ThreadPool {
    private:
        std::vector<std::thread> workers_;
        std::mutex readyqueue_mutex_;
        std::mutex waitqueue_mutex_;
        std::mutex counter_mutex_;
        std::condition_variable task_in_readyqueue_signal_;
        std::condition_variable task_in_waitqueue_signal_;
        std::condition_variable task_finish_signal_;
        std::unique_ptr<std::thread> control_thread_;
        std::map<TaskID, int> task_counter_;
        std::atomic<int> task_total_;
        bool stop_;
        std::queue<std::tuple<IRunnable*, int, int, TaskID>> tasks_readyqueue_;
        std::queue<std::tuple<IRunnable*, int, TaskID>> tasks_waitqueue_;
        std::shared_ptr<DependencyGraph> task_graph_;
    public:
        ThreadPool(int num_threads, std::shared_ptr<DependencyGraph> task_graph);
        ~ThreadPool();
        //void InitTask(IRunnable* runnable, int cur_task_id, int num_total_tasks);
        void AddReadyTask(IRunnable* runnable, int sub_task_id, int num_total_tasks, TaskID task_id);
        void AddWaitTask(IRunnable* runnable, int num_total_tasks, TaskID task_id);
        int GetFinishTaskNum();
        void WaitAllTaskFinish();
};

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        static void ThreadRunnable(IRunnable* runnable, int task_start, int task_end, int num_total_tasks);
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

class DependencyGraph {
    private:
        std::map<TaskID, std::vector<int>> nexttasks_;
        std::map<TaskID, int> unfinish_dependent_tasks_;
        std::map<TaskID, bool> task_finish_status_;
        std::atomic_int task_counter_;
        // only one user can operate the graph
        std::mutex graph_mutex_;

    public:
        DependencyGraph();
        ~DependencyGraph();
        // create a task node in the graph and return the task Id
        TaskID InitTask();
        
        // build the dependency graph whith the new node.
        void AddTask(TaskID prev_task, TaskID next_task);
        
        // delete the task in dependency graph when task is done
        void FinishTask(TaskID finish_task);

        // check if all the previous dependent task has finished
        bool CheckDependency(TaskID check_task); 

        void print() {
            std::cout << "Print Dependency Graph" << std::endl;
            for(auto iter = this->nexttasks_.begin(); iter != this->nexttasks_.end(); iter++) {
                TaskID task_id = iter->first;
                auto next_tasks = iter->second;
                std::cout << "TaskID: " << task_id << std::endl;
                std::cout << "Next Tasks: " << std::endl;
                for(auto& task: next_tasks)
                    std::cout << task << " ";
                std::cout << std::endl << std::endl;
            }
            std::cout << "Finish Status" << std::endl;
            for(auto iter = this->task_finish_status_.begin(); iter != this->task_finish_status_.end(); iter++) {
                std::cout << "TaskID: " << iter->first << " Status: " << iter->second << std::endl;
            }
        }
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
        std::map<TaskID, int> num_total_tasks_;
        ThreadPool* threadpool_;

        std::condition_variable task_finish_;
        std::shared_ptr<DependencyGraph> task_graph_;
};



#endif
