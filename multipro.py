#from https://gist.github.com/blaylockbk/8b469f2c79660ebdd18915202e0802a6

import numpy as np
import inspect
from multiprocessing import Pool, cpu_count          # Multiprocessing
from multiprocessing.dummy import Pool as ThreadPool # Multithreading
import time

def multipro_helper(func, inputs, cpus=6, threads=None, verbose=True):
    """
    Multiprocessing and multithreading helper.

    Parameters
    ----------
    func : function
        A function you want to apply to each item in ``inputs``.
        If your function has many inputs, its useful to call a helper 
        function that unpacks the arguments for each input.
    inputs : list
        A list of input for the function being called.
    cpus : int or None
        Number of CPUs to use. Will not exceed maximum number available
        and will not exceed the length of ``inputs``.
        If None, will try to use multithreading.
    threads : int or None
        Number of threads to use. Will not exceed 50 and will not exceed
        the length of ``inputs``.
        If None, will try to do each task sequentially as a list 
        comprehension.
    """
    assert callable(func), f"👻 {func} must be a callable function."
    assert isinstance(inputs, list), f"👻 inputs must be a list."             

    timer = time.time()

    if threads is not None:
        #assert isinstance(threads, np.integer), f"👻 threads must be a int. You gave {type(threads)}"
        threads = np.minimum(threads, 50) # Don't allow more than 50 threads.
        threads = np.minimum(threads, len(inputs))
        if verbose: print(f'🧵 Multithreading {func} with [{threads:,}] threads for [{len(inputs):,}] items.', end=' ')
        with ThreadPool(threads) as p:
            results = p.map(func, inputs)
            p.close()
            p.join()
            
    elif cpus is not None:
        #assert isinstance(cpus, np.integer), f"👻 cpus must be a int. You gave {type(cpus)}"
        cpus = np.minimum(cpus, cpu_count())
        cpus = np.minimum(cpus, len(inputs))
        if verbose: print(f'🤹🏻‍♂️ Multiprocessing {func} with [{cpus:,}] CPUs for [{len(inputs):,}] items.', end=' ')
        with Pool(cpus) as p:
            results = p.map(func, inputs)
            p.close()
            p.join()
    else:
        if verbose: print(f'📏 Sequentially do {func} for [{len(inputs):,}] items.', end=' ')
        results = [func(i) for i in inputs]

    if verbose: print(f"Timer={time.time()-timer}")

    return results