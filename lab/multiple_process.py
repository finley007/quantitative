import os,time
from multiprocessing import Pool

def worker(arg):
  print("子进程开始执行>>> pid={},ppid={},编号{}".format(os.getpid(),os.getppid(),arg))
  time.sleep(1)
  print("子进程终止>>> pid={},ppid={},编号{}".format(os.getpid(),os.getppid(),arg))

def main():
  print("主进程开始执行>>> pid={}".format(os.getpid()))
  ps=Pool(5)
  for i in range(10):
    # ps.apply(worker,args=(i,))     # 同步执行
    ps.apply_async(worker,args=(i,)) # 异步执行
  # 关闭进程池，停止接受其它进程
  ps.close()
  # 阻塞进程
  ps.join()
  print("主进程终止")


if __name__ == '__main__':
#     t = time.perf_counter()
#     for i in range(10):
#         worker(i)
#     print(time.perf_counter() - t)
    
    t = time.perf_counter()
    main()
    print(time.perf_counter() - t)