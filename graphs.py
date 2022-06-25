import matplotlib.pyplot as plt


def draw_worker_graph():
    plt.plot([5, 10, 25, 50, 75, 100, 150], [.741, .763, .789, .803, .783, .808, .885])
    plt.ylabel('Točnost modela')
    plt.xlabel('Broj radnika')
    plt.axis([0, 150, 0, 1])
    plt.show()


def draw_time_graph():
    plt.plot([5, 10, 25, 50, 75, 100, 150], [20.235, 22.505, 32.525, 38.485, 39.233, 42.063, 73.005])
    plt.ylabel('Vrijeme izvođenja')
    plt.xlabel('Broj radnika')
    plt.axis([0, 150, 0, 75])
    plt.show()


if __name__ == '__main__':
    draw_time_graph()