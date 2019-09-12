from h5py import Dataset
from PIL import Image
from rdflib import Graph, URIRef, BNode, Literal, Namespace
from rdflib.namespace import FOAF, RDF

import h5py
import numpy as np
import os
import ols_client
import uuid
import os.path


def write_func(in_files, out_file, groups):
    """Write the HDF5 file based on the input files, 
    group names and attributes.

    TODO: Make sure everything works with non-string data i.e. images etc.

    Arguments:
        in_files: List of input file to add to the HDF5 file.
        out_file: HDF5 output file.
        groups: The group structure.

    Raises:
        FileNotFoundError: The entered file does not exist.
        RuntimeError: An error occured while generating the HDF5 file.
    """
    data_file = h5py.File(out_file, 'a')
    image_extensions = ['jpg', 'jpeg', 'png', 'bmp', 'tiff']
    count = 0
    try:
        if len(groups) == len(in_files):
            for in_file in in_files:
                if in_file.split('.')[-1] not in image_extensions:
                    try:
                        with open(in_file) as ocf:
                            data = ocf.read()
                        str_type = h5py.special_dtype(vlen=str)
                        dset = data_file.create_dataset(
                            groups[count] + in_file.split('/')[-1],
                            data=data, shape=(1,),
                            dtype=str_type
                        )
                        attributes = generate_attributes_to_add(
                            groups[count] + in_file.split('/')[-1])
                        for k, v in attributes.items():
                            dset.attrs[k] = v
                    except FileNotFoundError:
                        print(in_file, "not found")
                else:
                    dset = image_to_hdf5(in_file, data_file, groups[count])
                    attributes = generate_attributes_to_add(
                        groups[count] + in_file.split('/')[-1])
                    for k, v in attributes.items():
                        dset.attrs[k] = v
                if len(groups) == 1:
                    count = 0
                else:
                    count += 1
    except RuntimeError:
        pass


def generate_attributes_to_add(group_name):
    """ Search ontology lookup service and create a list of attributes 
    to add to a dataset. At the moment the information stored from OLS
    is the name, iri and description.

    Arguments:
        group_name: The name of the group in which the dataset is located.

    Returns:
        Dictionary of attributes. Key is the name and 
        the values are the description and iri.
    """
    attributes = {}
    while True:
        print()
        print("Enter attributes for " + group_name)
        print("Please enter the attribute name and search term")
        print("i.e. Format fasta")
        attr = input("")
        attr_name = attr.split(' ')[0]
        ontolist = ontologies(attr.split(' ')[1])
        print("ID -- Name -- Description -- IRI")
        print()
        for onto in enumerate(ontolist):
            print(onto[0], "--", onto[1][0], "--",
                  onto[1][1], "--", onto[1][2])
        select_onto = input(
            "Select an ontology description(s) to add (comma seperated): ")
        if "," in select_onto:
            added_onto_list = []
            for select in select_onto.split(','):
                added_onto = ontolist[int(select)]
                added_onto_list.append(added_onto[1] + "--" + added_onto[2])
            attributes[attr_name] = [added_onto_list]
        elif select_onto == '':
            pass
        else:
            added_onto = ontolist[int(select_onto)]
            attributes[attr_name] = [added_onto[1] + "--" + added_onto[2]]
        continue_attr = input("Add another attribute? (Y/N): ")
        if continue_attr == "y" or continue_attr == "Y":
            True
        else:
            break
    return attributes


def generate_groups_to_delete():
    """Enter the groups to delete and 
    return a list of groups to remove from the HDF5 file.

    Returns:
        A list with all entered groups to delete from the HDF5 file.
    """
    groups_to_delete = []
    while True:
        datasetname = input("Enter dataset name to delete: ")
        groups_to_delete.append(datasetname)
        continue_del = input("Add another dataset to delete? (Y/N): ")
        if continue_del == "y" or continue_del == "Y":
            True
        else:
            break
    return groups_to_delete


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
        dataset = node[:][0]
        write_dataset(dataset, name.split('/')[-2], name.split('/')[-1])
    else:
        pass


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


def write_dataset(dataset, folder, out_file):
    """Create files based on the available datasets in the HDF5 file.

    Arguments:
        dataset: Dataset from the HDF5 file.
        folder: Directory to save the new dataset file.
        out_file: Name of the new dataset file.
    """
    if not os.path.isdir(folder):
        os.makedirs(folder)
    try:
        with open(folder + '/' + out_file, 'w') as writefile:
            writefile.write(dataset)
    except TypeError:
        im = Image.fromarray(dataset.astype('uint8'))
        im.save(folder + '/' + out_file, "PNG")


def h5py_dataset_iterator(fc, prefix=''):
    """Iterate through file content and get the dataset paths in the HDF5 file.

    Arguments:
        fc: HDF5 file content.

    Keyword Arguments:
        prefix: Group name prefix. 
        The default is '' as there is no group available before first one.
        (default: {''})
    """
    for key in fc.keys():
        item = fc[key]
        path = '{}/{}'.format(prefix, key)
        if isinstance(item, h5py.Dataset):
            yield (path, item)
        elif isinstance(item, h5py.Group):
            yield from h5py_dataset_iterator(item, path)


def get_attr(hdf_file):
    """Print all attributes from input HDF5 file.

    Arguments:
        hdf_file: HDF5 file to get the attributes from.
    """
    datasets = []
    with h5py.File(hdf_file, 'r') as f:
        for (path, dummydset) in h5py_dataset_iterator(f):
            datasets.append(path)
        for dataset in datasets:
            print(dataset, "has the following attributes:")
            all_attr = list(f[dataset].attrs)
            for attr in all_attr:
                attr_value = str(f[dataset].attrs.get(attr)[0])
                print(attr + ": " + attr_value.split("--")[1])
            print()
            print()


def generate_rdf(hdf_file):
    """Generate an RDF file based on an HDF5 file.

    Arguments:
        hdf_file: HDF5 file to generate an RDF from.
    """
    rdf_file = hdf_file.split('/')[-1] + ".rdf"
    if os.path.isfile(rdf_file):
        print(hdf_file.split('/')[-1] + ".rdf already exists!")
        pass
    else:
        print("Making First RDF file!!!")
        open(rdf_file, 'a').close()
    datasets = []
    g = Graph().parse(rdf_file, format='turtle')
    c = 0
    DCTERMS = Namespace('http://purl.org/dc/terms/')
    HDF2RDF = Namespace("http://example.org/hdf2rdf/")
    ISA = Namespace('http://purl.org/isaterms/')
    VOID = Namespace('http://rdfs.org/ns/void#')
    DCAT = Namespace('http://www.w3.org/ns/dcat#')
    RDF = Namespace('http://www.w3.org/1999/02/22-rdf-syntax-ns#')
    g.bind('dcterms', DCTERMS)
    g.bind('hdf2rdf', HDF2RDF)
    g.bind('isa', ISA)
    g.bind('void', VOID)
    g.bind('dcat', DCAT)
    g.bind('rdf', RDF)
    with h5py.File(hdf_file, 'r') as f:
        for (path, dummydset) in h5py_dataset_iterator(f):
            datasets.append(path)
        if os.path.isfile(rdf_file):
            with open(rdf_file) as rdfile:
                contents = rdfile.read()
                for dataset in datasets:
                    if 'file://' + rdf_file + '#' + dataset in contents:
                        print('file://' + rdf_file + '#' + dataset, "is already in RDF")
                    else:
                        uid_str = uuid.uuid4().urn
                        identifier = uid_str[9:]
                        isa_tab = dataset.split('/')
                        catalog = "/"
                        for cat in isa_tab[1:-1]:
                            catalog += (cat + "/")
                        all_attr = list(f[dataset].attrs)
                        g.add(
                            (
                                URIRef('file://' + rdf_file + '#' + dataset),
                                URIRef(DCTERMS + 'isPartOf'),
                                Literal(catalog)
                            )
                        )
                        g.add(
                            (
                                URIRef('file://' + rdf_file + '#' + dataset),
                                URIRef(RDF + 'type'),
                                URIRef(DCAT + 'Dataset')
                            )
                        )
                        g.add(
                            (
                                URIRef('file://' + rdf_file + '#' + dataset),
                                URIRef(DCAT + 'dataset'),
                                Literal(isa_tab[-1])
                            )
                        )
                        g.add(
                            (
                                URIRef('file://' + rdf_file + '#' + dataset),
                                URIRef(DCAT + 'title'),
                                Literal(isa_tab[-1].split('.')[0])
                            )
                        )
                        g.add(
                            (
                                URIRef('file://' + rdf_file + '#' + dataset),
                                URIRef(DCAT + 'identifier'),
                                Literal(identifier)
                            )
                        )
                        g.add(
                            (
                                URIRef('file://' + rdf_file + '#' + dataset),
                                URIRef(DCTERMS + 'format'),
                                Literal(isa_tab[-1].split('.')[-1])
                            )
                        )
                        for attr in all_attr:
                            f[dataset].attrs.get(attr)
                            predicate = URIRef(
                                HDF2RDF + attr.replace(" ",  "-"))
                            attr_value = f[dataset].attrs.get(attr)[0].split("--")
                            g.add(
                                (
                                    URIRef('file://' + rdf_file +
                                           '#' + dataset),
                                    predicate,
                                    Literal(attr_value[1])
                                )
                            )
                            c += 1
                        g.parse(hdf_file.split('/')
                                [-1] + ".rdf", format="turtle")
            g.serialize(destination=hdf_file.split('/')[-1] + ".rdf", format="turtle")


def delete_groups(hdf_file, groups_to_delete):
    """Delete specific datasets based on user input.

    Arguments:
        hdf_file: The HDF5 file.
        groups_to_delete: A list of groups to delete from the HDF5 file.
    """
    with h5py.File(hdf_file,  "a") as f:
        for group_name in groups_to_delete:
            del f[group_name]
            print(group_name, "deleted!")


def image_to_hdf5(filename, f, group):
    """Generate an HDF5 dataset from an image.

    Arguments:
        filename: Filename of the image.
        f: HDF5 file.
        group: HDF5 group name.

    Returns:
        HDF5 dataset in numpy.ndarray from the image.
    """
    img = Image.open(filename)
    image = np.array(img.getdata()).reshape(img.size[1], img.size[0], -1)
    data = np.array(image)
    dset = f.create_dataset(group + filename.split('/')
                            [-1], shape=(4, img.size[1], img.size[0], 4))
    dset[0] = data
    im = Image.fromarray(dset[0].astype('uint8'))
    im.save(filename)
    return dset


def ontologies(tag):
    """Search for ontologies based on user input.

    Arguments:
        tag: User input to find an ontology

    Returns:
        Ontology list with label, iri and description.
    """
    ols = ols_client.client.OlsClient()
    foundontologies = {}
    ontolist = []
    searchonto = ols.search(str(tag))
    for i in range(0, len(searchonto['response']['docs'])):
        try:
            iri = searchonto['response']['docs'][i]['iri']
            label = searchonto['response']['docs'][i]['label'].lower()
            description = searchonto['response']['docs'][i]['description'][0]
            if iri not in ontolist:
                if tag in label:
                    if iri not in ontolist:
                        foundontologies[label] = iri
                        ontolist.append((label, description, iri))
        except KeyError:
            pass
    return ontolist


if __name__ == "__main__":
    options = input(
        "1 = Create HDF file; 2 = Find datasets; 3 = Get attributes; 4 = Delete groups; 5 = Generate RDF: "
    )
    out_file = input("Enter HDF5 output path: ")
    if int(options) == 1:
        groups = []
        input_files = input(
            "Enter file paths (seperated by a space): ").replace('\\', '/')
        input_groups = input(
            "Enter groups (seperated by a space) i.e. /Project/Investigation/Study/Assay/: ")
        in_files = input_files.split(' ')
        groups = input_groups.split(' ')
        write_func(in_files, out_file, groups)
    elif int(options) == 2:
        with h5py.File(out_file, 'r') as f:
            f.visititems(find_datasets)
    elif int(options) == 3:
        get_attr(out_file)
    elif int(options) == 4:
        groups_to_delete = generate_groups_to_delete()
        delete_groups(out_file, groups_to_delete)
    elif int(options) == 5:
        generate_rdf(out_file)
