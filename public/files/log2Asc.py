import datetime
import re
import os
import argparse

# Global variable to store the reference epoch for absolute timestamp calculation
# This is crucial for consistency with v1.asc's large float timestamps.
# The exact epoch is not explicitly stated in the C code, but 2000-01-01 is a common one.
REFERENCE_EPOCH = datetime.datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)

# Day and Month abbreviations for ASC header formatting (matching v1.asc)
DAY_ABBR = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

def parse_busmaster_log_timestamp_to_seconds(time_str):
    """
    Parses a Busmaster timestamp string (HH:MM:SS:ms) where HH can be > 23,
    and converts it to total seconds from the start of that day, plus milliseconds.
    This mimics the `nHours*3600 + nMin*60 + nSec + (nMicroSeconds / 1000.0)` logic
    from `nGetAscTimeStamp` in Log_Asc_Parser.y.

    Example: "94:56:24:3862" -> total seconds + fractional seconds
    """
    try:
        parts = time_str.split(':')
        if len(parts) != 4:
            raise ValueError(f"Timestamp string must have 4 parts (HH:MM:SS:ms), got {len(parts)}")

        total_hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2])
        # The 'ms' part in Busmaster log is 4 digits, representing milliseconds.
        # We need to convert it to microseconds for Python's datetime,
        # but for the final float output, we use it as milliseconds.
        milliseconds = int(parts[3]) 

        total_seconds = (total_hours * 3600) + (minutes * 60) + seconds + (milliseconds / 1000.0)
        return total_seconds
    except ValueError as e:
        raise ValueError(f"Invalid custom Busmaster timestamp format '{time_str}': {e}")

def convert_busmaster_log_to_asc(input_log_path, output_asc_path):
    """
    Manually parses a Busmaster .log file and converts it to a .asc file
    matching the format of the provided 'v1.asc' sample, referring to the
    logic in Log_Asc_Parser.c and Log_Asc_Parser.y.

    If the output_asc_path already exists and has content, new log entries
    will be appended as a new 'Begin TriggerBlock' section, without
    duplicating the main .asc header.

    Args:
        input_log_path (str): Path to the input Busmaster .log file.
        output_asc_path (str): Path for the output .asc file.

    Returns:
        bool: True if conversion was successful, False otherwise.
    """
    print(f"Starting conversion from '{input_log_path}' to '{output_asc_path}' (v1.asc format)...")

    messages = []
    # This will store the full datetime object parsed from the Busmaster log header
    log_start_datetime_from_header = None 
    
    # Flags to track if base mode and timestamp mode headers have been processed
    # These are commented out in the C code, but we'll use them to ensure we only print once per file.
    # For appending, we only print these if the output file is new/empty.
    number_mode_processed_for_file = False
    timestamp_mode_processed_for_file = False

    try:
        with open(input_log_path, 'r', encoding='utf-8', errors='ignore') as infile:
            for line_num, line in enumerate(infile):
                line = line.strip()
                if not line:
                    continue

                # Parse START DATE AND TIME from header (M:D:YYYY HH:MM:SS:ms)
                if "START DATE AND TIME" in line and log_start_datetime_from_header is None:
                    match = re.search(r'START DATE AND TIME (\d+:\d+:\d+ \d+:\d+:\d+:\d+)', line)
                    if match:
                        date_time_str = match.group(1)
                        try:
                            # Split date and time parts
                            date_parts_str, time_parts_str = date_time_str.split(' ')
                            month_str, day_str, year_str = date_parts_str.split(':')
                            hour_str, minute_str, second_str, ms_str = time_parts_str.split(':')
                            
                            log_start_datetime_from_header = datetime.datetime(
                                int(year_str), int(month_str), int(day_str),
                                int(hour_str), int(minute_str), int(second_str),
                                int(ms_str) * 100, # Convert 4-digit ms to 6-digit microseconds
                                tzinfo=datetime.timezone.utc # Make it UTC-aware
                            )
                            print(f"Detected log start time from header: {log_start_datetime_from_header}")
                        except ValueError as e:
                            print(f"Warning: Could not parse START DATE AND TIME from header line: {line} - {e}. Falling back to current time.")
                            log_start_datetime_from_header = datetime.datetime.now(datetime.timezone.utc) # Fallback, make it UTC-aware
                    continue # Skip header lines after parsing

                # Parse BASE mode (e.g., ***HEX*** or ***DEC***)
                if "BASE" in line and not number_mode_processed_for_file:
                    if "***HEX***" in line:
                        # We'll set a flag, actual writing happens in the file write section
                        number_mode_processed_for_file = "hex"
                    elif "***DEC***" in line:
                        number_mode_processed_for_file = "dec"
                    continue

                # Parse TIMESTAMP MODE (e.g., ***RELATIVE MODE*** or ***ABSOLUTE MODE***)
                if "TIMEMODE" in line and not timestamp_mode_processed_for_file:
                    if "***RELATIVE MODE***" in line:
                        timestamp_mode_processed_for_file = "relative"
                    elif "***ABSOLUTE MODE***" in line:
                        timestamp_mode_processed_for_file = "absolute"
                    continue

                # Check for the data line header to identify where data starts
                if "***<Time><Tx/Rx><Channel><CAN ID><Type><DLC><DataBytes>***" in line:
                    continue # Skip this header line in the output

                # Parse data lines
                # Regex for: TIMESTAMP DIRECTION CHANNEL CAN_ID_HEX FRAME_TYPE DLC DATA_BYTES
                # Example Busmaster log line: 94:56:24:3862 Tx 1 0x011 s 8 00 00 00 00 00 00 00 00
                match = re.match(r'(\d+:\d{2}:\d{2}:\d{4})\s+(Rx|Tx)\s+(\d+)\s+(0x[0-9a-fA-F]+)\s+([sr])\s+(\d+)\s+([0-9a-fA-F\s]+)', line)
                if match:
                    try:
                        time_str_log, direction, channel, can_id_hex, frame_type_bm, dlc_str, data_bytes_str = match.groups()
                        
                        # Convert Busmaster's custom timestamp string to total seconds (float)
                        total_seconds_from_log_start_of_day = parse_busmaster_log_timestamp_to_seconds(time_str_log)
                        
                        # Combine with the date from the log header to get a full absolute datetime object
                        if log_start_datetime_from_header is None:
                            # This should ideally not happen if header is well-formed, but as a fallback
                            log_start_datetime_from_header = datetime.datetime.now(datetime.timezone.utc) # Make fallback UTC-aware
                            print("Warning: Log start date not found in header. Using current UTC date for absolute timestamp calculation.")

                        # Calculate the full absolute datetime of the current message
                        # Create a UTC-aware datetime object for the start of that specific day
                        start_of_current_log_day_utc = datetime.datetime(
                            log_start_datetime_from_header.year, 
                            log_start_datetime_from_header.month, 
                            log_start_datetime_from_header.day,
                            0, 0, 0, 0, tzinfo=datetime.timezone.utc
                        )

                        # Add the total seconds offset to this UTC-aware start-of-day
                        current_absolute_datetime = start_of_current_log_day_utc + datetime.timedelta(seconds=total_seconds_from_log_start_of_day)

                        # Convert to seconds since the defined REFERENCE_EPOCH (for v1.asc style large float)
                        absolute_seconds_float = (current_absolute_datetime - REFERENCE_EPOCH).total_seconds()

                        dlc = int(dlc_str)
                        
                        # Format CAN ID: remove '0x' prefix and format to uppercase hex.
                        # v1.asc uses 3-digit padded hex for IDs like '011'.
                        can_id_int = int(can_id_hex, 16)
                        formatted_can_id = f"{can_id_int:X}" # Default to uppercase hex
                        if can_id_int <= 0xFFF: # If it's a standard CAN ID (11-bit), pad to 3 digits
                             formatted_can_id = f"{can_id_int:03X}"
                        elif can_id_int <= 0x1FFFFFFF: # If it's an extended CAN ID (29-bit)
                             formatted_can_id = f"{can_id_int:X}" # Just use its hex representation

                        asc_direction = direction # Tx or Rx
                        asc_frame_type = "d" # Data frame
                        if frame_type_bm == 'r': # Remote Transmission Request
                            asc_frame_type = "r"

                        # Ensure data bytes are correctly space-separated and uppercase
                        formatted_data_bytes = ' '.join(data_bytes_str.upper().split())

                        # v1.asc message format: TIMESTAMP CHANNEL ID DIRECTION FRAME_TYPE DLC DATA_BYTES
                        messages.append(f"{absolute_seconds_float:.6f} {channel} {formatted_can_id} {asc_direction} {asc_frame_type} {dlc} {formatted_data_bytes}")
                    except ValueError as e:
                        print(f"Warning: Data parsing error on line {line_num + 1}: {e} - Skipping line: {line}")
                    except Exception as e:
                        print(f"Warning: Unexpected error processing line {line_num + 1}: {e} - Skipping line: {line}")
                # Check for log end statement
                elif line.startswith("_[STOP LOGGING SESSION]_"):
                    # This marks the end of a logical block in Busmaster log, corresponds to End TriggerBlock
                    messages.append("END_OF_LOG_BLOCK_MARKER") # Use a special marker to handle blocks
                    continue
                else:
                    pass # Ignore lines that don't match data or known header patterns

        if not messages:
            print("No valid CAN messages found in the .log file to convert.")
            return False

        # Determine if we should append or overwrite
        append_mode = os.path.exists(output_asc_path) and os.path.getsize(output_asc_path) > 0
        file_open_mode = 'a' if append_mode else 'w'

        with open(output_asc_path, file_open_mode, encoding='utf-8') as outfile:
            if not append_mode:
                # ASC Header - only write if it's a new file or empty
                if log_start_datetime_from_header is None:
                    log_start_datetime_from_header = datetime.datetime.now(datetime.timezone.utc) # Make fallback UTC-aware
                    print("Using current UTC system time for ASC header date as no valid start date found in log.")

                # Format date to match 'v1.asc' - e.g., "Sun Aug 3 8:57:21 pm 2025"
                # Use strftime for most parts, then manually handle 12-hour format with am/pm
                day_of_week_abbr = DAY_ABBR[log_start_datetime_from_header.weekday()]
                month_abbr = MONTH_ABBR[log_start_datetime_from_header.month - 1]
                day_of_month = log_start_datetime_from_header.day
                
                hour_12 = log_start_datetime_from_header.hour % 12
                if hour_12 == 0:
                    hour_12 = 12 # 12 AM/PM
                am_pm = "am" if log_start_datetime_from_header.hour < 12 else "pm"
                
                formatted_date_str = f"{day_of_week_abbr} {month_abbr} {day_of_month} {hour_12}:{log_start_datetime_from_header.minute:02d}:{log_start_datetime_from_header.second:02d} {am_pm} {log_start_datetime_from_header.year}"

                outfile.write(f"date {formatted_date_str}\n")
                
                # Write base mode (hex/dec)
                if number_mode_processed_for_file == "hex":
                    outfile.write("base hex  ")
                elif number_mode_processed_for_file == "dec":
                    outfile.write("base dec  ")
                else:
                    # Default if not found in log header
                    outfile.write("base hex  ") 
                
                # Write timestamp mode and other fixed header lines
                if timestamp_mode_processed_for_file == "relative":
                    outfile.write("timestamps relative\n")
                elif timestamp_mode_processed_for_file == "absolute":
                    outfile.write("timestamps absolute\n")
                else:
                    # Default if not found in log header (v1.asc uses absolute)
                    outfile.write("timestamps absolute\n")

                outfile.write("no internal events logged\n") # Matches v1.asc exactly
                outfile.write("// version 7.1.0\n") # Matches v1.asc exactly
            
            # Write Begin TriggerBlock, messages, and End TriggerBlock for each session
            # If appending, we need to remove the last "End TriggerBlock\n\n" from the file first
            # to insert the new block correctly. This is complex with simple file operations.
            # A simpler approach for appending is to just add the new block.
            # If the user wants precise concatenation, they might need to clear the file first.
            if append_mode:
                # To truly append without nested blocks, we'd need to read the whole file,
                # remove the last End TriggerBlock, then append. For simplicity and
                # to match the "new file add headers" interpretation for blocks,
                # we'll just append a new block.
                outfile.write("\n") # Add a newline for separation if appending
            
            outfile.write("Begin TriggerBlock\n")

            # Write messages, handling potential internal block markers if any
            for msg_line in messages:
                if msg_line == "END_OF_LOG_BLOCK_MARKER":
                    outfile.write("End TriggerBlock\n\n")
                    outfile.write("Begin TriggerBlock\n") # Start a new block if marker is found
                else:
                    outfile.write(msg_line + "\n")

            # ASC Footer
            outfile.write("End TriggerBlock\n")

        print(f"Successfully created/appended to '{output_asc_path}'.")
        return True

    except FileNotFoundError:
        print(f"Error: Input file '{input_log_path}' not found.")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during .log to .asc conversion: {e}")
        return False

# --- Main execution ---
if __name__ == "__main__":
    # Setup argument parser
    parser = argparse.ArgumentParser(description="Convert Busmaster .log files to Vector .asc format (v1.asc style).")
    parser.add_argument("input_log_path", help="Path to the input Busmaster .log file.")
    parser.add_argument("output_asc_path", help="Path for the output Vector .asc file.")
    
    args = parser.parse_args()

    # Use arguments from the command line
    input_busmaster_log = args.input_log_path
    output_asc_file = args.output_asc_path

    # --- Create a dummy Busmaster .log file for demonstration if it doesn't exist ---
    # This part is for demonstration/testing. In a real scenario, you'd provide your actual log file.
    if not os.path.exists(input_busmaster_log):
        print(f"Input log file '{input_busmaster_log}' not found. Creating a dummy one for demonstration.")
        os.makedirs(os.path.dirname(input_busmaster_log) or '.', exist_ok=True)
        
        # Use a fixed time for the dummy log file to ensure consistent absolute timestamps
        # This will help in verifying the conversion to the large float format
        fixed_start_time = datetime.datetime(2025, 8, 3, 8, 57, 21, 0) # Matching v1.asc header date/time
        
        # Example of a log entry with 94 hours, relative to fixed_start_time
        # This means the "94" is an offset from the start of the log.
        # So, 94:56:24:3862 means 94 hours, 56 minutes, 24 seconds, 386.2 milliseconds
        # from the start of the log session.
        # The first message in v1.asc is 341768.679900.
        # If the log starts at 8:57:21 AM on Aug 3, 2025, then 94 hours later is:
        # Aug 3, 8:57:21 AM + 94 hours = Aug 7, 6:57:21 AM
        # So, the 94:56:24:3862 timestamp in the log *itself* is an absolute time,
        # relative to the *start of the day* of the log's start date.

        dummy_log_content = f"""
***BUSMASTER Ver 1.9.0 Beta 1*** ***PROTOCOL CAN*** ***NOTE: PLEASE DO NOT EDIT THIS DOCUMENT*** ***[START LOGGING SESSION]*** ***START DATE AND TIME {fixed_start_time.strftime('%m:%d:%Y %H:%M:%S:%f')[:-2]}*** ***HEX*** ***SYSTEM MODE*** ***START CHANNEL BAUD RATE*** ***CHANNEL 1 - Virtual #0 (Channel 0), Serial Number- 0, Firmware- 0x00000000
***CHANNEL 2 - Virtual #0 (Channel 1), Serial Number- 0, Firmware- 0x00000000 0x00000000 - 500000 bps*** ***END CHANNEL BAUD RATE*** ***START DATABASE FILES (DBF/DBC)*** ***END DATABASE FILES (DBF/DBC)*** ***<Time><Tx/Rx><Channel><CAN ID><Type><DLC><DataBytes>***
{fixed_start_time.strftime('%H:%M:%S:%f')[:-2]} Rx 1 0x100 s 8 01 02 03 04 05 06 07 08
{datetime.time(94, 56, 24, 386200).strftime('%H:%M:%S:%f')[:-2]} Tx 1 0x011 s 8 00 00 00 00 00 00 00 00
{datetime.time(94, 56, 24, 400000).strftime('%H:%M:%S:%f')[:-2]} Rx 1 0x300 s 8 11 22 33 44 55 66 77 88
{datetime.time(94, 56, 24, 500000).strftime('%H:%M:%S:%f')[:-2]} Tx 1 0x400 r 0
_[STOP LOGGING SESSION]_
"""
        with open(input_busmaster_log, "w") as f:
            f.write(dummy_log_content.strip())
        print("Dummy Busmaster .log file created for demonstration.")

    # --- Perform the conversion ---
    conversion_success = convert_busmaster_log_to_asc(input_busmaster_log, output_asc_file)

    if conversion_success:
        print("\nConversion process completed successfully!")
        print(f"Your .asc file should be located at: {os.path.abspath(output_asc_file)}")
    else:
        print("\nConversion process failed. Please check the error messages above.")

    # --- Clean up dummy files (optional) ---
    # if os.path.exists(input_busmaster_log) and "dummy" in input_busmaster_log: # Add robust check
    #     os.remove(input_busmaster_log)
    #     print(f"Removed dummy .log file: {input_busmaster_log}")
