#!/usr/bin/python

# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import sys
import codecs

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    # 1. We create a dictionary with all the different words in the file
    current_word = ""
    current_amount = 0

    # 2. We traverse the file content, to populate my_dict
    for line in my_input_stream:
        # 2.1. We get the info from the line
        line = line.replace("\n", "")
        info = line.split("\t")

        word = info[0]
        num_appearances = int(info[1][1:-1])

        # 2.2. If current word == word then we update the number of appearances
        if (word == current_word):
            current_amount = current_amount + num_appearances
        # 2.3. If the word is a new one
        else:
            # 2.3.1. We first print the previous word
            if (current_word != ""):
                my_str = current_word + "\t(" + str(current_amount) + ")\n"
                my_output_stream.write(my_str)

            # 2.3.2. We reset the current word to be the new one
            current_word = word
            current_amount = num_appearances

    # 3. We print the very last word
    if (current_word != ""):
        my_str = current_word + "\t(" + str(current_amount) + ")\n"
        my_output_stream.write(my_str)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(local_False_Cloudera_True,
            my_reducer_input_parameters,
            input_file_example,
            output_file_example
           ):

    # 1. We select the input and output streams based on our working mode
    my_input_stream = None
    my_output_stream = None

    # 1.1: Local Mode --> We use the debug files
    if (local_False_Cloudera_True == False):
        my_input_stream = codecs.open(input_file_example, "r", encoding='utf-8')
        my_output_stream = codecs.open(output_file_example, "w", encoding='utf-8')

    # 1.2: Cloudera --> We use the stdin and stdout streams
    else:
        my_input_stream = sys.stdin
        my_output_stream = sys.stdout

    # 2. We trigger my_reducer
    my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Local Mode or Cloudera
    local_False_Cloudera_True = False

    # 2. Debug Names
    input_file_example = "../my_result/my_sort_results.txt"
    output_file_example = "../my_result/my_reducer_results.txt"

    # 3. my_reducer.py input parameters
    # We list the parameters here

    # We create a list with them all
    my_reducer_input_parameters = []

    # 4. We call to my_main
    my_main(local_False_Cloudera_True,
            my_reducer_input_parameters,
            input_file_example,
            output_file_example
           )
