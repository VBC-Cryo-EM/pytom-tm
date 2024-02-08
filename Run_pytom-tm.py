#!/usr/bin/env python3

import os
import argparse
import starfile
import subprocess
import sys

# Set up argument parsing
parser = argparse.ArgumentParser(description='Process STAR file and run pytom_match_template.py.')
parser.add_argument('--input-tomos', '-i', help='Path to relion ReconstructTomograms job,, i,.e /path/to/relion/ReconstructTomograms/job020 ', required=True)
parser.add_argument('--template', '-t' , help='Path to the mrc file of your template matching reference', required=True)
parser.add_argument('--mask','-m', help='Path to the mask file applied to your template matching reference', required=True)
parser.add_argument('--non-spherical-mask', action='store_true', help='Use a non-spherical mask during template matching')
parser.add_argument('--output-dir', '-o', help='Path to output directory where pytom results are saved. If not gived, the default is input_tomos/pytom_tm', default=None)
parser.add_argument('--per-tilt-weighting', action='store_true', help='Enable the per tilt weigthting option by ctf and accumualted dose')
parser.add_argument('--amplitude-contrast', type=float, default=0.08, help='Amplitude contrast (default: 0.08)')
parser.add_argument('--spherical-abberation', type=float, default=2.7, help='Spherical abberation (default: 2.7)')
parser.add_argument('--voltage', type=int, default=300, help='Voltage value (default: 300)')
parser.add_argument('--angular-search', type=float, required=True, help='Angular search value. Allowed options are 3.00, 7.00, 11.00, 12.85, 17.86, 18.00, 19.95, 25.25, 38.53, 35.76,50.00, 90.00')
parser.add_argument('--voxel-size', type=float, required=False, help='Voxel size value of input tomograms and template (must be identical). If not given it will be read form the mrc header')
parser.add_argument('--high-pass', type=int, required=False, help='Apply a high-pass filter to the tomogram and template to reduce correlation with large low frequency variations. Value is a resolution in A, e.g. 500 could be appropriate as the CTF is often incorrectly modelled up to 50nm.')
parser.add_argument('--low-pass', type=int, required=False, help='Apply a low-pass filter to the tomogram and template. Generally desired if the template was already filtered to a certain resolution. Value is the resolution in A.')
parser.add_argument('--volumesplit', '-s', nargs='*', help='Split the volume into smaller parts for the search, can be relevant if the volume does not fit into GPU memory. Format is x y z, e.g. --volume-split 1 2 1 will split y into 2 chunks, resulting in 2 subvolumes. --volume-split 2 2 1 will split x and y in 2 chunks, resulting in 2 x 2 = 4 subvolumes.', default=[])
parser.add_argument('-n', '--number_of_particles', type=int, required=True, help='Number of particles')
parser.add_argument('-r', '--particle_radius', type=int, required=True, help='Particle radius in pixels')
parser.add_argument('--cutoff', '-c', type=float, help='Cutoff value to be used in pytom_extract_candidates', required=False)

parser.add_argument('--batch-size', '-b' , type=int, default=10, help='Run pytom on this many tomograms per job. I.e if you have 50 tomograms in the relion  idrectory, a batch size of 10 will generate 5 SLURM scripts, each procesing 10 tomograms.')
#SLURM options 
parser.add_argument('--mem', type=str, default='30G', help='Memory for SLURM job (default: 30G)')
parser.add_argument('--qos', type=str, default='short', help='QoS for SLURM job (default: short)')
parser.add_argument('--gres', type=int, default=1, help='How many GPUs to use per tomogram. Default is 1. if larger, make sure to also give GPU IDs')
parser.add_argument('--gpu-ids','-g', nargs='*', help='Which GPUs to use. Default is 0. Needs to be given when --gres>1', default=[0])


#option to only make scripts for analysis
parser.add_argument('--skip-matching', action='store_true', help='Skip generating the template matching SLURM scripts and only write out extract_candidates and estimate_roc. useful when you want to change --number_of_particles or --particle_radius for analysis of the template matching results')
# Add --force flag argument to trigger re-processing of already matched tilt series 
parser.add_argument('--force', action='store_true', help='Reprocess all tilt series regardless of existing job JSON files.')

# Parse arguments
args = parser.parse_args()

# Path to pytom container
container_path = '/resources/containers/pytom_tm.sif'

# Checking if input files/directories exist
def check_file_exists(file_path, description):
    if not os.path.exists(file_path):
        print(f"Error: The specified {description}, '{file_path}', does not exist.")
        exit(1)

# Perform checks
check_file_exists(args.input_tomos, "input tomograms directory")
check_file_exists(args.template, "template file")
check_file_exists(args.mask, "mask file")


# Function to find the closest value in the list to the user's input
def find_closest_value(input_value, allowed_values):
    closest_value = min(allowed_values, key=lambda x: abs(x - input_value))
    if closest_value != input_value:
        formatted_input = "{:.2f}".format(input_value)
        formatted_closest = "{:.2f}".format(closest_value)
        print(f"Warning: Provided angular search value {formatted_input} adjusted to closest allowed value {formatted_closest}.")
    return "{:.2f}".format(closest_value)  # Return the formatted closest value

#Extract tilt series meta data from star file and write into auxilary files for pytom tempalte matching
def process_tilt_series_data(tilt_series_name, args):
    aux_dir = os.path.join(args.output_dir, 'pytom_aux')
    try:
        os.makedirs(aux_dir, exist_ok=True)
        star_file_path = os.path.join(args.input_tomos, 'tilt_series', f'{tilt_series_name}.star')
        data = starfile.read(star_file_path)
        #read star file columns
        tilt_angles = data['rlnTomoNominalStageTiltAngle']
        defocus_values = data['rlnDefocusU'] * 0.1
        dose_values = data['rlnMicrographPreExposure']

        #define paths for output files
        tilt_angles_file = os.path.join(aux_dir, f'{tilt_series_name}_for_pytom_tilt.tlt')
        defocus_values_file = os.path.join(aux_dir, f'{tilt_series_name}_for_pytom_defocus.txt')
        dose_values_file = os.path.join(aux_dir, f'{tilt_series_name}_for_pytom_dose.txt')

        tilt_angles.to_csv(tilt_angles_file, index=False, header=False)
        defocus_values.to_csv(defocus_values_file, index=False, header=False)
        dose_values.to_csv(dose_values_file, index=False, header=False)
    except Exception as e:
        sys.stderr.write(f"Failed to process tilt series data for {tilt_series_name}: {e}\n")
        sys.exit(1)
    return True


# Function to generate template matching command
def generate_pytom_command(tilt_series_name, args):
    aux_dir = os.path.join(args.output_dir, 'pytom_aux')  # Directory for auxiliary files
    input_tomo_file = os.path.join(args.input_tomos, 'tomograms', f'rec_{tilt_series_name}.mrc')

    # Adjust paths to point to files in the aux_dir
    output_file_tilt = os.path.join(aux_dir, f'{tilt_series_name}_for_pytom_tilt.tlt')
    output_file_defocus = os.path.join(aux_dir, f'{tilt_series_name}_for_pytom_defocus.txt')
    output_file_dose = os.path.join(aux_dir, f'{tilt_series_name}_for_pytom_dose.txt')
    job_json_file = f'{input_tomo_file}_job.json'

    command_components = [
        'pytom_match_template.py',
        f"--template {args.template}",
        f"--mask {args.mask}",
        "--non-spherical-mask" if args.non_spherical_mask else "",
        f"--tomogram {input_tomo_file}",
        f"--destination {args.output_dir}",
        f"--tilt-angles {output_file_tilt}",
        f"--angular-search {find_closest_value(args.angular_search, allowed_angular_search_values)}",
        f"--voxel-size {args.voxel_size}" if args.voxel_size else "",
        f"--high-pass {args.high_pass}" if args.high_pass else "",
        f"--low-pass {args.low_pass}" if args.low_pass else "",
        "--per-tilt-weighting" if args.per_tilt_weighting else "",
        f"--dose-accumulation {output_file_dose}",
        f"--defocus-file {output_file_defocus}",
        f"--amplitude-contrast {args.amplitude_contrast}",
        f"--spherical-abberation {args.spherical_abberation}",
        f"--voltage {args.voltage}",
        f"--volume-split {' '.join(map(str, args.volumesplit))}" if args.volumesplit else "",
        "--spectral-whitening",
        f"--gpu-ids {' '.join(map(str, args.gpu_ids))}" if args.gpu_ids else "",
        #'"'
    ]
 
 
    # Close the command string
    command = ' '.join(filter(None, command_components))
    final_command = f"apptainer run --nv {container_path} '{command}'"

    return final_command

# Function to generate the estimate ROC command
def generate_estimate_roc_command(tilt_series_name, args):
    job_json_file = os.path.join(args.output_dir, f'rec_{tilt_series_name}_job.json')
    return f"apptainer run --nv {container_path} 'pytom_estimate_roc.py -j {job_json_file} -n {args.number_of_particles} -r {args.particle_radius}'"

# Function to generate the extract candidates command
def generate_extract_candidates_command(tilt_series_name, args):
    job_json_file = os.path.join(args.output_dir, f'rec_{tilt_series_name}_job.json')
    command = f"apptainer run --nv {container_path} 'pytom_extract_candidates.py -j {job_json_file} -n {args.number_of_particles} -r {args.particle_radius}"
    # Include the cutoff value in the command if it's supplied
    if args.cutoff is not None:
        command += f" -c {args.cutoff}"
    command += "'"
    return command


# Convert file paths to absolute paths
args.input_tomos = os.path.abspath(args.input_tomos)
args.template = os.path.abspath(args.template) if args.template else None
args.mask = os.path.abspath(args.mask) if args.mask else None
args.output_dir = os.path.abspath(args.output_dir) if args.output_dir else os.path.join(args.input_tomos, 'pytom_tm')

# Create the output directory if it does not exist
os.makedirs(args.output_dir, exist_ok=True)
print(f"Output directory {args.output_dir} is ready.")

# Read the STAR file located at [input_tomos]/tomograms.star
tomograms_star_file = os.path.join(args.input_tomos, 'tomograms.star')
try:
    tomograms_data = starfile.read(tomograms_star_file)
    tilt_series_names = tomograms_data['rlnTomoName']
except Exception as e:
    print(f"Error reading tomograms.star file: {e}")
    exit()
print(f"Reading STAR file from {tomograms_star_file}.")

# After reading tilt series names from the STAR file
tilt_series_names = tomograms_data['rlnTomoName'].tolist()

# Initialize a list to hold tilt series names for which to generate commands
tilt_series_to_process = []

# Check for existing job JSON files
for tilt_series_name in tilt_series_names:
    job_json_path = os.path.join(args.output_dir, f'rec_{tilt_series_name}_job.json')
    if os.path.exists(job_json_path) and not args.force:
        print(f"Skipping {tilt_series_name}: job JSON file already exists. Use --force to reprocess.")
    else:
        tilt_series_to_process.append(tilt_series_name)

# List of allowed angular search values
allowed_angular_search_values = [7.00, 35.76, 19.95, 90.00, 18.00, 12.85, 38.53, 11.00, 17.86, 25.25, 50.00, 3.00]

# Construct the SLURM header
slurm_header = f'''#!/usr/bin/env bash

#SBATCH --job-name=pytom
#SBATCH --partition=g
#SBATCH --cpus-per-task=4
#SBATCH --gres=gpu:{args.gres}
#SBATCH --nodes=1
#SBATCH --mem={args.mem}
#SBATCH --qos={args.qos}
'''

# Initialize command storage
commands_per_tilt_series = []
estimate_roc_commands = []
extract_candidates_commands = []

# Loop through each tilt series name
print("Processing tilt series data...")

for tilt_series_name in tilt_series_to_process:
    if process_tilt_series_data(tilt_series_name, args):
        if not args.skip_matching:
            commands_per_tilt_series.append(generate_pytom_command(tilt_series_name, args))
        estimate_roc_commands.append(generate_estimate_roc_command(tilt_series_name, args))
        extract_candidates_commands.append(generate_extract_candidates_command(tilt_series_name, args))


#functions to write SLURM submission script files 
def write_sbatch_file(commands, file_name, submission_dir):
    full_path = os.path.join(submission_dir, file_name)
    with open(full_path, 'w') as file:
        file.write(slurm_header)
        file.write('\n'.join(commands))
    print(f"SBATCH script written to: {full_path}")

#functions to split the script into several batches with a size specified in the --batch-size argument
def write_script_batches(commands_list, script_name, output_dir):
    submission_dir = os.path.join(output_dir, 'submission_scripts')
    os.makedirs(submission_dir, exist_ok=True)  # Ensure the directory exists

    if args.batch_size and len(commands_list) > args.batch_size:
        for i in range(0, len(commands_list), args.batch_size):
            batch_commands = commands_list[i:i + args.batch_size]
            batch_file_name = f'{script_name}_batch{i//args.batch_size}.sbatch'
            write_sbatch_file(batch_commands, batch_file_name, submission_dir)
    else:
        file_name = f'{script_name}.sbatch'
        write_sbatch_file(commands_list, file_name, submission_dir)

# Write script batches conditionally
if not args.skip_matching:
    write_script_batches(commands_per_tilt_series, 'pytom_template_matching', args.output_dir)
    print("Template matching scripts written.")

write_script_batches(estimate_roc_commands, 'pytom_estimate_roc', args.output_dir)
print("Estimate ROC scripts written.")

write_script_batches(extract_candidates_commands, 'pytom_extract_candidates', args.output_dir)
print("Extract candidates scripts written.")

print("All processes completed successfully.")
