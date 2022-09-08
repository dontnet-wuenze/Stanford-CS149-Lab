#include <iostream>
#include "tasksys.h"
#include "itasksys.h"
#include <mutex>
#include <thread>
#include <tuple>
#include <vector>

using std::thread;
using std::vector;

/*
 * ================================================================
 * My Implementation of ThreadPool 
 * ================================================================
 */

ThreadPool::ThreadPool(int num_threads): stop_(false) {
    for(int i = 0; i < num_threads; i++) {
        this->workers_.emplace_back([this] {
            while(true) {

                std::unique_lock<std::mutex> lock(this->mutex_);
                this->task_inqueue_signal_.wait(lock, [this] {
                    return this->stop_ || !this->tasks_queue_.empty();
                });
                if(this->stop_ && this->tasks_queue_.empty())
                    break;
                auto [runnable, task_id, num_total_tasks] = tasks_queue_.front();
                tasks_queue_.pop();
                lock.unlock();
                runnable->runTask(task_id, num_total_tasks);
                // 结束后 task_counter_++
                {
                    //std::unique_lock<std::mutex> lock(this->counter_mutex_);
                    this->task_counter_++;
                    if(this->task_counter_ == num_total_tasks)
                        this->task_finish_signal_.notify_all();
                }
            }
            return;
        });
    }
}

void ThreadPool::Init() {
    //std::cout << this->task_counter_ << std::endl;
    this->task_counter_ = 0;
    return;
}

int ThreadPool::GetFinishTaskNum() {
    return this->task_counter_;
}

void ThreadPool::WaitAllTaskFinish(int num_total_tasks) {
    std::unique_lock<std::mutex> lock(this->counter_mutex_);
    this->task_finish_signal_.wait(lock, [num_total_tasks, this] {
        //std::cout << this->task_counter_ << " " << num_total_tasks << std::endl;
        if(this->task_counter_ == num_total_tasks)
            return true;
        return false;
    });
}

void ThreadPool::AddTask(IRunnable *runnable, int task_id, int num_total_tasks) {
    std::unique_lock<std::mutex> lock(this->mutex_);
    if(this->stop_)
        throw std::runtime_error("enqueue on stopped ThreadPool");
    this->tasks_queue_.push(std::tuple<IRunnable*, int, int>(runnable, task_id, num_total_tasks));
    lock.unlock();
    this->task_inqueue_signal_.notify_one();
    return;
}

ThreadPool::~ThreadPool() {
    {
        std::unique_lock<std::mutex> lock(mutex_);
        this->stop_ = true;
    }
    this->task_inqueue_signal_.notify_all();
    for(auto &worker : workers_) {
        worker.join();
    }
    return;
}

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads): num_threads_(num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    //std::cout << "In Serial" << std::endl;
    //std::cout << num_total_tasks << std::endl;
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::ThreadRunnable(IRunnable* runnable, int task_start, int task_end, int num_total_tasks) {
    for(int i = task_start; i < task_end; i++)
        runnable->runTask(i, num_total_tasks);
    return ;
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }
    vector<thread> thread_array;

    int perthread = num_total_tasks / this->num_threads_;
    //std::cout << perthread << " " << this->num_threads_ << " " << num_total_tasks << " " << std::endl;
    
    for(int i = 0; i < this->num_threads_; i++) {
        int task_start = i * perthread;
        int task_end = (i + 1) * perthread;
        if(task_start > num_total_tasks)
            break;
        if(i == num_threads_ - 1)
            task_end = num_total_tasks;
        //thread_array.push_back(thread(&TaskSystemParallelSpawn::ThreadRunnable, runnable, task_start, task_end, num_total_tasks));
        thread_array.push_back(thread([runnable, task_start, task_end, num_total_tasks] ()-> void {
            for(int i = task_start; i < task_end; i++)
                runnable->runTask(i, num_total_tasks);
            return ; 
        }));
    }
    for(int i = 0; i < this->num_threads_; i++)
        thread_array[i].join(); 
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->threadpool_ = new ThreadPool(num_threads);
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    delete this->threadpool_;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    //for (int i = 0; i < num_total_tasks; i++) {
    //    runnable->runTask(i, num_total_tasks);
    //}
    //this->threadpool_ = new ThreadPool(this->num_threads_);

    //std::cout << "In ThreadPoolSleeping" << std::endl;
    //std::cout << num_total_tasks << std::endl;
    //std::cout << "start a bulk of tasks" << std::endl;
    threadpool_->Init();
    for(int i = 0; i < num_total_tasks; i++) {
        threadpool_->AddTask(runnable, i, num_total_tasks);
    }
    threadpool_->WaitAllTaskFinish(num_total_tasks);
    //delete this->threadpool_;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
