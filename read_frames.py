import threading
import queue
import cv2
import time
from collections import deque
import os
import av
import numpy as np


def fetch_frames(stream_index, stream_url, frame_queue, stop_event, frame_counters, show_videos):
    """Fetch frames from the given stream and put them into the queue."""
    container = None

    # Set options
    options = {
        'rtsp_transport': 'udp',  # use 'tcp' if you want to force TCP mode
        'max_delay': '1000000',   # 5 seconds max delay (in microseconds). Adjust accordingly.
        'probesize': '20000000',   # 5 MB probesize. Adjust as needed.
    }

    while not stop_event.is_set():
        try:
            if container is None:
                frame_counters[stream_index]['last_reset'] = time.time()
                container = av.open(stream_url, options=options)

            for frame in container.decode(video=0):  # video=0 selects the video stream
                frame_counters[stream_index]['total'] += 1
                frame_counters[stream_index]['recent'].append(time.time())
                
                if show_videos:
                    # Convert the PyAV frame to an image (numpy array) so that you can display it or process it using OpenCV
                    img = frame.to_image()  # PIL image
                    frame_array = np.array(img)  # Convert PIL image to numpy array

                    # Only keep the latest frame in the queue
                    if not frame_queue.empty():
                        try:
                            frame_queue.get_nowait()
                        except queue.Empty:
                            pass
                    frame_queue.put(frame_array)

        except Exception as e:
            # print(e)
            # frame_counters[stream_index]['errors'].append(e.__str__())
            frame_counters[stream_index]['errors'].append(e)
            if container:
                container.close()
                container = None
            time.sleep(10)

    if container:
        container.close()

def clear_terminal():
    os.system('cls' if os.name == 'nt' else 'clear')

def display_fps(frame_counters):
    while True:
        time.sleep(1)
        clear_terminal()
        for idx, counter in enumerate(frame_counters):
            url = counter['url']
            total_frames = counter['total']
            recent_frames = counter['recent']
            longest_duration = counter['longest_duration']
            now = time.time()

            # Remove frames older than n seconds
            fps_n = 10
            while recent_frames and now - recent_frames[0] > fps_n:
                recent_frames.popleft()

            avg_fps = total_frames / (now - start_time)
            recent_fps = len(recent_frames) / fps_n

            time_since_restart = int(now - counter['last_reset'])

            if time_since_restart > longest_duration:
                longest_duration = time_since_restart
                counter['longest_duration'] = longest_duration

            errors_str = "" if len(counter['errors'])==0 else counter['errors'][-1]
            num_errors = len(counter['errors'])
            print(f"{idx} : {url}\tFPS: {recent_fps:.2f}\tlongest duration: {longest_duration}s\tlast reset: {time_since_restart}s ago\terrors: {num_errors}->{errors_str}")


if __name__ == "__main__":
    rtsp_streams = [
        'rtsp://127.0.0.1:8554/stream1',
        'rtsp://127.0.0.1:8554/stream2',
        'rtsp://127.0.0.1:8554/stream3',
        'rtsp://127.0.0.1:8554/stream4',
        'rtsp://127.0.0.1:8554/stream5',
    ]
    stress_repeats = 4

    rtsp_streams = rtsp_streams * stress_repeats

    show_videos = False
    
    frame_queues = [queue.Queue(maxsize=1) for _ in rtsp_streams]
    stop_events = [threading.Event() for _ in rtsp_streams]
    
    # Initialize frame counters for each stream
    frame_counters = [{"url":_, "total": 0, "recent": deque(), "last_reset": time.time(), "longest_duration":0, "errors":[]} for _ in rtsp_streams]

    # Start threads for fetching frames from each stream
    threads = [threading.Thread(target=fetch_frames, args=(i, rtsp_stream, frame_queues[i], stop_events[i], frame_counters, show_videos))
               for i, rtsp_stream in enumerate(rtsp_streams)]
    for thread in threads:
        thread.start()

    # Start the FPS display thread
    start_time = time.time()
    fps_thread = threading.Thread(target=display_fps, args=(frame_counters,))
    fps_thread.start()

    try:
        while True:
            if show_videos:
                for i, frame_queue in enumerate(frame_queues):
                    if not frame_queue.empty():
                        frame = frame_queue.get()
                        cv2.imshow(f'RTSP Stream {i}', frame)

            # # Press 'q' to close the window
            # if cv2.waitKey(1) & 0xFF == ord('q'):
            #     break
    except KeyboardInterrupt:
        print("KeyboardInterrupt")

    # Stop all fetching threads
    for stop_event in stop_events:
        stop_event.set()
    for thread in threads:
        thread.join()

    cv2.destroyAllWindows()
    fps_thread.join()