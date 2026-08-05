from PIL import Image, ImageSequence

def trim_gif_start(input_path, output_path, seconds_to_remove=3.0):
    """
    Removes the specified number of seconds from the beginning of a GIF.
    """
    ms_to_remove = seconds_to_remove * 1000
    
    with Image.open(input_path) as img:
        info = img.info
        default_duration = info.get('duration', 100) # Fallback if duration is missing
        
        frames = []
        durations = []
        elapsed_time = 0
        
        # Iterate through all frames to find the cut-off point
        for frame in ImageSequence.Iterator(img):
            # Get current frame's duration in ms
            current_duration = frame.info.get('duration', default_duration)
            
            # Keep the frame only if we have passed the trim threshold
            if elapsed_time >= ms_to_remove:
                # copy() prevents the frame data from closing with the context manager
                frames.append(frame.copy())
                durations.append(current_duration)
                
            elapsed_time += current_duration

        if not frames:
            print("Error: The GIF is shorter than the requested trim duration.")
            return

        # Save the trimmed frames as a new animated GIF
        frames[0].save(
            output_path,
            save_all=True,
            append_images=frames[1:],
            duration=durations,       # Matches original variable frame speeds
            loop=info.get('loop', 0), # Retains original loop settings
            optimize=True
        )
        print(f"Successfully saved trimmed GIF to {output_path}")

# Example usage:
trim_gif_start("canviz_demo.gif", "canviz_demo_trimmed.gif", seconds_to_remove=5)
