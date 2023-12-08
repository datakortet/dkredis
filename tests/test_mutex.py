# import threading
# import time
#
# from dkredis import mutex
#
# counter = 0
#
#
# def use_mutex():
#     global counter
#     for i in range(10):
#         with mutex('d', 1, 4, True, 1) as lock:
#             start = counter
#             # time.sleep(0.1)
#             print(f'counter is: {counter}, was: {start}, will be: {start+1}')
#             counter = start + 1
#
#
# def test_mutex():
#     global counter
#     counter = 0
#     threads = [threading.Thread(target=use_mutex) for _ in range(10)]
#     for t in threads:
#         t.start()
#     for t in threads:
#         t.join()
#     assert counter == 10
#
#
# if __name__ == '__main__':
#     test_mutex()
