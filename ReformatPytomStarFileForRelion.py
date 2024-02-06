#!/usr/bin/env python3

import starfile
import os
import glob
import argparse

def replace_text_in_file(file_path, replacements):
    """
    Read a file, perform text replacements, and save the changes back to the file.
    """
    with open(file_path, 'r') as file:
        file_content = file.read()
    
    for old, new in replacements.items():
        file_content = file_content.replace(old, new)
    
    with open(file_path, 'w') as file:
        file.write(file_content)

def process_star_file(input_star_path, output_star_path):
    # Define text replacements
    replacements_first_pass = {
        '_ptm': '_rln',
        'MicrographName': 'TomoName',
        'data_': 'data_particles',
        'rec_': ''
    }
    
    # Read the entire content of the original STAR file and perform first pass replacements
    with open(input_star_path, 'r') as file:
        file_content = file.read()
    
    for old, new in replacements_first_pass.items():
        file_content = file_content.replace(old, new)
    
    # Save the modified content to a temporary file for further processing
    temp_star_path = output_star_path + '.temp'
    with open(temp_star_path, 'w') as file:
        file.write(file_content)
    
    # Now read and process the temporary STAR file
    data = starfile.read(temp_star_path)
    
    # Perform multiplication on specified columns if they exist
    required_columns = ['rlnCoordinateX', 'rlnCoordinateY', 'rlnCoordinateZ', 'rlnDetectorPixelSize']
    if all(column in data.columns for column in required_columns):
        for coord_column in ['rlnCoordinateX', 'rlnCoordinateY', 'rlnCoordinateZ']:
            data[coord_column] = data[coord_column] * data['rlnDetectorPixelSize']
    
    # Write modifications to the output STAR file
    starfile.write(data, output_star_path)
    
    # Second pass of text replacements on the final output file
    replacements_second_pass = replacements_first_pass
    replace_text_in_file(output_star_path, replacements_second_pass)

    print(f"Modifications saved to {output_star_path}")
    
    # Remove the temporary file
    os.remove(temp_star_path)

def process_directory(directory_path):
    star_files = glob.glob(os.path.join(directory_path, '*.star'))
    for input_star_path in star_files:
        output_star_path = os.path.splitext(input_star_path)[0] + '_for_relion.star'
        process_star_file(input_star_path, output_star_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process STAR files in a directory to replace text and modify coordinates.")
    parser.add_argument("directory_path", type=str, help="Path to the directory containing STAR files.")
    
    args = parser.parse_args()
    
    process_directory(args.directory_path)
