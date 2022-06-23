import matplotlib.pyplot as plt


def draw_worker_graph():
    plt.plot([5, 10, 25, 50, 100], [.867, .815, .761, .765, .686])
    plt.ylabel('Točnost modela')
    plt.xlabel('Broj radnika')
    plt.axis([0, 100, 0, 1])
    plt.show()


def draw_iteration_graph():
    plt.plot([2, 4, 6, 8, 10], [.604, .708, .812, .872, .899])
    plt.ylabel('Točnost modela')
    plt.xlabel('Broj iteracija')
    plt.axis([2, 10, 0, 1])
    plt.show()


if __name__ == '__main__':
    draw_iteration_graph()