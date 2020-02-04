import multiprocessing as mp
from time import time

from progressbar import ProgressBar, Percentage, Bar, Timer, ETA, FileTransferSpeed



def split_list(lst, nums):
    nums = max(1, nums)
    rows = len(lst)
    interval = round(rows / nums)

    lsts = []
    for i in range(nums):
        s = i * interval
        if i != nums - 1:
            e = (i + 1) * interval
        else:
            e = rows
        lsts.append(lst[s:e])
    while [] in lsts:
        lsts.remove([])
    return lsts


class MultiProc:
    def __init__(self, ncpu, func, pairs, name: str = None):
        self.name = name
        self.func = func
        self.pairs = pairs
        self.parse_ncpu(ncpu)
        self.widgets = ['Progress: ', Percentage(), ' ', Bar('#'), ' ', Timer(),
                        ' ', ETA(), ' ', FileTransferSpeed()]
        self.verbose = True
        self.updated_indeces = []

    def parse_ncpu(self, ncpu_: int):
        self.is_multi = False
        if ncpu_ < 0:
            self.ncpu = min(mp.cpu_count(), len(self.pairs))
            self.is_multi = True
        elif ncpu_ in (0, 1):
            self.is_multi = False
            self.ncpu = 1
        else:
            self.is_multi = True
            self.ncpu = ncpu_
        if self.is_multi:
            self.pool = mp.Pool(self.ncpu)
        print('cpus count:', self.ncpu)

    def get_n_steps(self):
        '''判断总共需要执行多少步（slef.pairs）'''
        return len(self.pairs)
        # raise NotImplementedError()

    def get_args(self, raw_args):
        '''raw_args=pairs[i]'''
        return raw_args
        # raise NotImplementedError()

    def after_process(self):
        '''对res_list进行后处理'''
        return self.res_list

    def update(self, i):
        m = (i / self.n_steps) * 100
        m = int(m)
        if not 0 <= m < 100:
            return
        if self.verbose:
            self.pbar.update(m)
        else:
            if m % 5 != 0:
                return
            if m not in self.updated_indeces:
                self.pbar.update(m)
                self.updated_indeces.append(m)

    def main_process(self):
        # todo : 限制进度条的打印频率
        if self.name:
            print(f'开始 {self.name} 任务')
        self.start = time()
        # init data
        self.pool_list = []
        self.res_list = []
        self.n_steps = self.get_n_steps()
        self.pbar = ProgressBar(widgets=self.widgets, maxval=100).start()
        # multi processing
        for i, raw_arg in enumerate(self.pairs):
            args = self.get_args(raw_arg)
            if self.is_multi:
                res = self.pool.apply_async(self.func, args)
                self.pool_list.append(res)
            else:
                res = self.func(*args)
                self.res_list.append(res)
                self.update(i)
        if self.is_multi:
            for i, res in enumerate(self.pool_list):
                self.res_list.append(res.get())
                self.update(i)
            self.pool.close()
            self.pool.join()
        # after process
        ret = self.after_process()
        # close pbar
        self.pbar.finish()
        if self.name:
            print(f'完成 {self.name} 任务')
        self.time = time() - self.start
        print(f'持续时间: {self.time:.2f}s')
        return ret
