#!/usr/bin/env python3
from sys import stdin
from matplotlib import pyplot as plt
import numpy as np


plt.rcParams['text.usetex'] = True


def main():
    all_data = []
    for line in stdin:
        level_cache_miss = list(map(int, line.split(' ')))
        all_data.append(level_cache_miss[:3])
    cache_index = np.arange(len(all_data))
    plt.plot(cache_index, all_data, label=['LV0', 'LV1', 'LV2'])
    plt.title("Cache misses by queries subject to normal distribution")
    plt.xlabel(r"$\sigma$")
    plt.ylabel("Cache misses")
    plt.legend()
    plt.show()


if __name__ == '__main__':
    main()
