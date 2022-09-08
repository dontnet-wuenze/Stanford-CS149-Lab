#include <condition_variable>
#include <iostream>
#include "tasksys.h"
#include "itasksys.h"
#include <memory>
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

ThreadPool::ThreadPool(int num_threads, std::shared_ptr<DependencyGraph> task_graph): stop_(false) {
    this->task_total_ = 0;
    this->task_graph_ = task_graph;

    for(int i = 0; i < num_threads; i++) {
        this->workers_.emplace_back([this] {
            while(true) {
                std::unique_lock<std::mutex> lock(this->readyqueue_mutex_);
                this->task_in_readyqueue_signal_.wait(lock, [this] {
                    return this->stop_ || !this->tasks_readyqueue_.empty();
                });
                if(this->stop_ && this->tasks_readyqueue_.empty())
                    break;
                auto [runnable, sub_task_id, num_total_tasks, task_id] = tasks_readyqueue_.front();
                tasks_readyqueue_.pop();
                lock.unlock();
                runnable->runTask(sub_task_id, num_total_tasks);
                // 结束后 task_counter_++
                {
                    std::unique_lock<std::mutex> lock(this->counter_mutex_);
                    this->task_counter_[task_id]++;
                    //std::cout << "subtask finish" << std::endl;
                    //std::cout << "task_id: " << task_id << " sub_task_id: " << sub_task_id << " total_subtasks: " << num_total_tasks << " subtask count: " << this->task_counter_[task_id] << std::endl;
                    if(this->task_counter_[task_id] == num_total_tasks) {
                        this->task_total_--;
                        this->task_graph_->FinishTask(task_id);
                        this->task_finish_signal_.notify_all();
                        //std::cout<< this->task_total_ << std::endl;
                    }
                }
            }
            return;
        });
    }

    this->control_thread_ = std::make_unique<std::thread>([this]{
        while(true) {
            std::unique_lock<std::mutex> lock(this->waitqueue_mutex_); 
            this->task_finish_signal_.wait(lock, [this] {
                if(this->stop_)
                    return true;
                if(this->tasks_waitqueue_.empty())
                    return false;
                auto&& [runnable, num_total_tasks, task_id] = this->tasks_waitqueue_.front();
                if(this->task_graph_->CheckDependency(task_id))
                    return true;
                return false;
            });
            if(this->stop_)
                break;
            // if check pass means the front task of wait queue can start;       
            while(!this->tasks_waitqueue_.empty()) {
                auto&& [runnable, num_total_tasks, task_id] = this->tasks_waitqueue_.front();
                if(!this->task_graph_->CheckDependency(task_id))
                    break;
                for(int i = 0; i < num_total_tasks; i++)
                    this->AddReadyTask(runnable, i, num_total_tasks, task_id);
                this->tasks_waitqueue_.pop();
            }
        }
    });

}


void ThreadPool::AddWaitTask(IRunnable *runnable, int num_total_tasks, TaskID task_id) {
    std::unique_lock<std::mutex> counter_lock(this->counter_mutex_);
    this->task_counter_[task_id] = 0;
    counter_lock.unlock();
    this->task_total_++;
    std::unique_lock<std::mutex> lock(this->waitqueue_mutex_);
    if(this->stop_)
       throw std::runtime_error("enqueue on stopped ThreadPool"); 
    this->tasks_waitqueue_.push(std::tuple<IRunnable*, int, TaskID>(runnable, num_total_tasks, task_id));
    lock.unlock();
    this->task_finish_signal_.notify_all();
    return;
}

void ThreadPool::AddReadyTask(IRunnable *runnable, int sub_task_id, int num_total_tasks, TaskID task_id) {
    std::unique_lock<std::mutex> lock(this->readyqueue_mutex_);
    if(this->stop_)
        throw std::runtime_error("enqueue on stopped ThreadPool");
    this->tasks_readyqueue_.push(std::tuple<IRunnable*, int, int, TaskID>(runnable, sub_task_id, num_total_tasks, task_id));
    lock.unlock();
    this->task_in_readyqueue_signal_.notify_one();
    return;
}

void ThreadPool::WaitAllTaskFinish() {
    while(this->task_total_) {
    }
    return;
}

ThreadPool::~ThreadPool() {
    std::cout << "In ThreadPool Destruction" << std::endl;
    {
        std::unique_lock<std::mutex> wait_lock(this->waitqueue_mutex_);
        std::unique_lock<std::mutex> ready_lock(this->readyqueue_mutex_);
        this->stop_ = true;
    }
    this->task_in_readyqueue_signal_.notify_all();
    this->task_finish_signal_.notify_all();
    for(auto &worker : workers_) {
        worker.join();
    }
    this->control_thread_->join();
    return;
}

/*
 * ================================================================
 * My Implementation of Dependency Graph 
 * ================================================================
 */
DependencyGraph::DependencyGraph() {
    this->task_counter_ = 0;
}

DependencyGraph::~DependencyGraph() {}

TaskID DependencyGraph::InitTask() {
    std::unique_lock<std::mutex> lock(graph_mutex_);
    TaskID cur_task = this->task_counter_++;
    this->nexttasks_[cur_task] = vector<TaskID>();
    this->unfinish_dependent_tasks_[cur_task] = 0;
    this->task_finish_status_[cur_task] = false;
    return cur_task;
}

void DependencyGraph::AddTask(TaskID prev_task, TaskID next_task) {
    // 加锁
    std::unique_lock<std::mutex> lock(graph_mutex_);
    if(this->task_finish_status_[prev_task] == false) {
        this->nexttasks_[prev_task].push_back(next_task);
        this->unfinish_dependent_tasks_[next_task]++;
    }
}

void DependencyGraph::FinishTask(TaskID finish_task) {
    std::unique_lock<std::mutex> lock(graph_mutex_);
    //std::cout << "TaskID: finish" << std::endl;
    //this->print();
    auto next_tasks = this->nexttasks_[finish_task];
    for(auto& next_task: next_tasks) 
        this->unfinish_dependent_tasks_[next_task]--;
    this->task_finish_status_[finish_task] = true;
    //this->print();
}

bool DependencyGraph::CheckDependency(TaskID check_task) {
    std::unique_lock<std::mutex> lock(graph_mutex_);
    if(this->unfinish_dependent_tasks_[check_task] == 0)
        return true;
    return false;
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
    this->task_graph_ = std::make_shared<DependencyGraph>();
    std::cout << "Build Graph Success" << std::endl;
    this->threadpool_ = new ThreadPool(num_threads, this->task_graph_);
    std::cout << "Build ThreadPool Success" << std::endl;
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
    //threadpool_->Init();
    //threadpool_->WaitAllTaskFinish(num_total_tasks);
    //delete this->threadpool_;
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

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
    // first add all task in the graph;
    //std::cout << "In runAsync" << std::endl;
    TaskID cur_task_id;
    cur_task_id = this->task_graph_->InitTask();
    //std::cout<< "cur_task_id: " << cur_task_id << std::endl;
    //threadpool_->InitTask(runnable, cur_task_id, num_total_tasks);
    for(int i = 0; i < deps.size(); i++)
        this->task_graph_->AddTask(deps[i], cur_task_id);
    //this->task_graph_->print();
    this->threadpool_->AddWaitTask(runnable, num_total_tasks, cur_task_id);
    return cur_task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    this->threadpool_->WaitAllTaskFinish();

    return;
}
