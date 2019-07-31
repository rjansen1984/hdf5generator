from h5py import Dataset

import h5py
import numpy as np
import os


def write_func(in_files, out_file, group, attributes):
    """Write the HDF5 file based on the input files, 
    group names and attributes.
    
    TODO: Make sure everything works with non-string data i.e. images etc.
    
    Arguments:
        in_files: List of input file to add to the HDF5 file.
        out_file: HDF5 output file.
        group: The group structure.
        attributes: Attributes to add to the HDF5 file.

    Raises:
        FileNotFoundError: The entered file does not exist.
        RuntimeError: An error occured while generating the HDF5 file.
    """
    data_file = h5py.File(out_file, 'a')
    try:
        for group in groups:
            for in_file in in_files:
                if group.split('/')[-2] in in_file:
                    try:
                        with open(in_file) as ocf:
                            data = ocf.read()
                            str_type = h5py.new_vlen(str)
                            data_file.create_dataset(
                                group + in_file.split('/')[-1],
                                data=data, shape=(1,),
                                dtype=str_type
                            )
                    except FileNotFoundError:
                        print(in_file, "not found")
                else:
                    pass
    except RuntimeError:
        pass
    for k, v in attributes.items():
        data_file.attrs[k] = v
    with h5py.File(out_file,  "a") as f:
        f['/Nanopore/PRIMUL/RB/RB01/assembly.fasta'].attrs["EDAM"] = "This should be FASTA and assembly EDAM IDs"


def find_datasets(name, node):
    """Read the nodes from the h5py visititems function
    and check if the node is a dataset.
    
    TODO: See if this works with different array types.
    FIXME: Add option for different array types.
    
    Arguments:
        name: Name from h5py visititems.
        node: The group node from h5py visititems.
    """
    if isinstance(node, h5py.Dataset):
        write_dataset(node[:][0], name.split('/')[-2], name.split('/')[-1])
    else:
        get_groups(name)


def get_groups(name):
    """Get the ISA structure based on the HDF5 group name.

    TODO: Return ISA structure with group names.
    TODO: Find out want we want with this functionality.

    Arguments:
        name: The name of the HDF5 group.
    """
    groups = {}
    isa_structure = ["Project", "Investigation", "Study", "Assay"]
    groupname = name.split('/')[-1]
    groups[isa_structure[len(name.split('/'))-1]] = groupname
    print(groups)


def write_dataset(dataset, folder, out_file):
    """Create files based on the available datasets in the HDF5 file.

    Arguments:
        dataset: Dataset from the HDF5 file.
        folder: Directory to save the new dataset file.
        out_file: Name of the new dataset file.
    """
    if not os.path.isdir(folder):
        os.makedirs(folder)
    with open(folder + '/' + out_file, 'w') as writefile:
        writefile.write(dataset)


def get_attr(hdf_file):
    """Print all attributes from input HDF5 file.

    Arguments:
        hdf_file: HDF5 file to get the attributes from.
    """
    with h5py.File(hdf_file, 'r') as f:
        print(hdf_file, "has the following attributes:")
        print()
        all_attr = list(f.attrs)
        for attr in all_attr:
            print(attr + ":")
            attrlist = f.attrs.get(attr)
        for att in attrlist:
            print(att)


def delete_dataset(hdf_file, datasets_to_delete):
    """Delete specific datasets based on user input.
    
    Arguments:
        hdf_file: The HDF5 file.
        datasets_to_delete: A list of datasets to delete from the HDF5 file.
    """
    with h5py.File(hdf_file,  "a") as f:
        for datasetname in datasets_to_delete:
            del f[datasetname]


if __name__ == "__main__":
    options = input(
        "Please enter the number of the option you want to use. 1=Create HDF5 file 2=Find datasets 3=Get attributes from HDF5 file 4=Delete datasets: ")
    out_file = input("Enter HDF5 filename: ")
    if int(options) == 1:
        input_files = input("Enter file paths (seperated by a space): ")
        input_groups = input("Enter groups (seperated by a space): ")
        in_files = input_files.split(' ')
        groups = input_groups.split(' ')
        attributes = {}
        while True:
            attr = input(
                "Enter attributes i.e. EDAM http://edamontology.org/topic_3168,http://edamontology.org/format_1929: ")
            if "," in attr:
                attributes[attr.split(' ')[0]] = attr.split(' ')[1].split(',')
            elif attr == '':
                pass
            else:
                attributes[attr.split(' ')[0]] = [attr.split(' ')[1]]
            continue_attr = input("Add another attribute? (Y/N): ")
            if continue_attr == "y" or continue_attr == "Y":
                True
            else:
                break
        write_func(in_files, out_file,
                   groups, attributes)
    elif int(options) == 2:
        with h5py.File(out_file, 'r') as f:
            f.visititems(find_datasets)
    elif int(options) == 3:
        get_attr(out_file)
    elif int(options) == 4:
        datasets_to_delete = []
        while True:
            datasetname = input("Enter dataset name to delete: ")
            datasets_to_delete.append(datasetname)
            continue_del = input("Add another dataset to delete? (Y/N): ")
            if continue_del == "y" or continue_del == "Y":
                True
            else:
                break
        delete_dataset(out_file, datasets_to_delete)
