from h5py import Dataset
from PIL import Image
from rdflib import Graph, URIRef, BNode, Literal, Namespace
from rdflib.namespace import FOAF, RDF
from subprocess import call

import sys
import h5py
import numpy as np
import os
import ols_client
import uuid
import os.path
import requests
import json
import datetime


def write_groups(out_file, groupname):
    """Write the groups and attributes to an HDF file.
    
    Arguments:
        out_file: HDF5 output file.
        groupname: Name of a group that will be added to the HDF.
    """
    print("To create a single group please just enter the main group name i.e. Group Name")
    print('To create a subgroup to an exisitng group, please enter /Group Name/Subgroup Name/etc/etc/')
    print()    
    attributes = {}
    print("Enter attributes for", groupname)
    meta = input("Is there a metadata file? (Y/N): ")
    if meta == "Y" or meta == "y":
        metapath = input("Enter metadata file path: ")
        with open(metapath, 'r') as metafile:
            for line in metafile:
                line = line.split('\t')
                item = line[0].strip('\n')
                value = line[-1].strip('\n')
                if item in attributes.keys():
                    attributes[item].append(value)
                else:
                    attributes[item] = [value]
    else:
        input_attributes = input("Enter an attribute followed by a value. i.e. Project Name: iknowit, Date: 04-11-2019: ")
        for attribute in input_attributes.split(','):
            attribute = attribute.split(':')
            attributes[attribute[0].strip(' ')] = attribute[1].strip(' ')
    data_file = h5py.File(out_file, 'a')
    dset = data_file.create_group(groupname)
    for k, v in attributes.items():
        dset.attrs[k] = v


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
    attributes = dict()
    print("Enter attributes for " + group_name)
    meta = input("Is there a metadata file? (Y/N): ")
    if meta == "Y" or meta == "y":
        metapath = input("Enter metadata file path: ")
        with open(metapath, 'r') as metafile:
            for line in metafile:
                line = line.split('\t')
                item = line[0].strip('\n')
                value = line[-1].strip('\n')
                if item in attributes.keys():
                    attributes[item].append(value)
                else:
                    attributes[item] = [value]
    else:
        while True:
            print()
            print("Enter attributes for " + group_name)
            ontosearching = input("Search ontologies? Y/N: ")
            if ontosearching == "Y" or ontosearching == "y":
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
                if select_onto == '':
                    pass
                else:
                    for select in select_onto.split(','):
                        added_onto = ontolist[int(select)]
                        attributes[attr_name + "Description"] = [added_onto[1]]
                        attributes[attr_name + "IRI"] = [added_onto[2]]
            else:
                naturalis_term = input("Add a Naturalis specimen ID: ")
                naturalis = search_naturalis(naturalis_term)
                for nitem, nvalue in naturalis.items():
                    attributes[nitem] = [nvalue]
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
                for val in f[dataset].attrs.get(attr):
                    attr_value = str(val)
                    print(attr + ": " + attr_value)
            print()
            print()


def get_namespaces(g):
    """Creates RDF namespaces and binds them to the graph.
    
    Arguments:
        g: Triple store graph

    Returns:
        List with the used RDF namespaces.
    """
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
    
    return [DCTERMS, HDF2RDF, ISA, VOID, DCAT, RDF]


def add_isa_triples(g, hdf_file, isa_title, isa_labels, isa_tab, count):
    """Adds the ISA triples to the graph.
    
    Arguments:
        g: Triple store graph.
        hdf_file: The HDF file.
        isa_title: The ISA title.
        Either the investigation, study or assay title)
        isa_labels: The label that belongs to the title.
        isa_tab: The ISA structure as a list.
        count: Number that is linked to the ISA catalog.
    """
    namespaces = get_namespaces(g)
    if count == 1:
        g.add(
            (
                URIRef(hdf_file + "#" + isa_title),
                URIRef(namespaces[-1] + 'type'),
                Literal(isa_labels.get(count))
            )
        )
        g.add(
            (
                URIRef(hdf_file + "#" + isa_title),
                URIRef(namespaces[0] + 'hasPart'),
                Literal(hdf_file + "#" + isa_tab[count + 1])
            )
        )
    else:
        g.add(
            (
                URIRef(hdf_file + "#" + isa_title),
                URIRef(namespaces[-1] + 'type'),
                URIRef(namespaces[2] + isa_labels.get(count))
            )
        )
        g.add(
            (
                URIRef(hdf_file + "#" + isa_title),
                URIRef(namespaces[0] + 'isPartOf'),
                Literal(hdf_file + "#" + isa_tab[count - 1])
            )
        )
        try:
            g.add(
                (
                    URIRef(hdf_file + "#" + isa_title),
                    URIRef(namespaces[0] + 'hasPart'),
                    Literal(hdf_file + "#" + isa_tab[count + 1])
                )
            )
        except IndexError:
            pass


def add_hdf_trples(g, hdf_file, dataset, catalog, isa_tab, identifier):
    """Adds the triples to the graph based on the attributes in the HDF.
    
    Arguments:
        g: Triple store graph
        hdf_file: The HDF file.
        dataset: The name of the dataset.
        catalog: The complete HDF structure linked to the dataset.
        isa_tab: The ISA structure as a list.
        identifier {[type]} -- [description]
    """
    namespaces = get_namespaces(g)
    g.add(
        (
            URIRef(hdf_file + "#" + dataset),
            URIRef(namespaces[0] + 'isPartOf'),
            Literal(catalog)
        )
    )
    g.add(
        (
            URIRef(hdf_file + "#" + dataset),
            URIRef(namespaces[-1] + 'type'),
            URIRef(namespaces[4] + 'Dataset')
        )
    )
    g.add(
        (
            URIRef(hdf_file + "#" + dataset),
            URIRef(namespaces[4] + 'dataset'),
            Literal(isa_tab[-1])
        )
    )
    g.add(
        (
            URIRef(hdf_file + "#" + dataset),
            URIRef(namespaces[4] + 'title'),
            Literal(isa_tab[-1].split('.')[0])
        )
    )
    g.add(
        (
            URIRef(hdf_file + "#" + dataset),
            URIRef(namespaces[4] + 'identifier'),
            Literal(identifier)
        )
    )
    g.add(
        (
            URIRef(hdf_file + "#" + dataset),
            URIRef(namespaces[0] + 'format'),
            Literal(isa_tab[-1].split('.')[-1])
        )
    )


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
        print("Making RDF file...")
        open(rdf_file, 'a').close()
    datasets = []
    g = Graph().parse(rdf_file, format='turtle')
    c = 0
    namespaces = get_namespaces(g)
    with h5py.File(hdf_file, 'r') as f:
        for (path, dummydset) in h5py_dataset_iterator(f):
            datasets.append(path)
        if os.path.isfile(rdf_file):
            with open(rdf_file) as rdfile:
                contents = rdfile.read()
                for dataset in datasets:
                    if rdf_file in contents:
                        print(dataset, "is already in RDF")
                    else:
                        count = 0
                        uid_str = uuid.uuid4().urn
                        identifier = uid_str[9:]
                        isa_tab = dataset.split('/')
                        catalog = "/"
                        isa_labels = {
                            1: "project",
                            2: "investigation",
                            3: "study",
                            4: "assay"
                        }
                        for cat in isa_tab[1:-1]:
                            count += 1
                            add_isa_triples(g, hdf_file, cat, isa_labels, isa_tab, count)
                            catalog += (cat + "/")
                        all_attr = list(f[dataset].attrs)
                        add_hdf_trples(g, hdf_file, dataset, catalog, isa_tab, identifier)
                        for attr in all_attr:
                            f[dataset].attrs.get(attr)
                            predicate = URIRef(
                                namespaces[1] + attr.replace(" ",  "-"))
                            try:
                                attr_value = f[dataset].attrs.get(attr)
                            except AttributeError:
                                literal_object = Literal(attr_value[0])
                            if len(attr_value) > 1:
                                for value in attr_value:
                                    literal_object = Literal(value)
                                    g.add(
                                        (
                                            URIRef(hdf_file + "#" + dataset),
                                            predicate,
                                            literal_object
                                        )
                                    )
                            else:
                                literal_object = Literal(attr_value[0])
                                g.add(
                                    (
                                        URIRef(hdf_file + "#" + dataset),
                                        predicate,
                                        literal_object
                                    )
                                )
                            c += 1
                        g.parse(hdf_file.split('/')
                                [-1] + ".rdf", format="turtle")
            g.serialize(destination=hdf_file.split('/')[-1] + ".rdf", format="turtle")
            print("Finished!")


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


def query_rdf(rdf_file, predicate):
    """Query the generated RDF file based on a predicate
    
    TODO: Add option to query based on other search terms.

    Arguments:
        rdf_file: Path of the RDF file to query.
        predicate: The entered predicate used to query the RDF.
    """
    g = Graph().parse(rdf_file, format='turtle')
    for s,p,o in g:
        if predicate in p:
            name = s.split('#')[1]
            print(p, "======", o)
            print("Available in file", s.split('#')[1])
            print()
    with h5py.File(rdf_file[:-4], 'r') as f:
        print(f[name][:])
    

def search_naturalis(query):
    """Query the naturalis specimen collection to add as attributes to an HDF.
    
    Arguments:
        query: Search term to retrieve the specimen information.

    Returns:
        A dictionary containing information from the naturalis collection.
    """
    naturalis = {}
    get_specimen = "https://api.biodiversitydata.nl/v2/specimen/find/" + query
    headers = {
        'accept': 'application/json',
        'charset': 'UTF-8',
    }
    naturalis_json = requests.get(get_specimen, headers=headers).content
    jnl = json.loads(naturalis_json)
    for item in jnl:
        if item != "sourceSystem" or item != "identifications":
            naturalis[item] = str(jnl[item])
        if item == "identifications":
            for subitem in jnl[item]:
                for x in range(len(jnl[item])):
                    for subitem in jnl[item][x]:
                        naturalis[subitem] = str(jnl[item][x][subitem])
    return naturalis


def help():
    """Printing the help text when user selected the --help option or 
    enetered an option that does not exist.
    """
    print("This script can be used to generate HDF files and convert them to triples.")
    print()
    print("Usage:") 
    print("Create group in HDF file --> python hdf5generator.py --create_group <HDF path>")
    print("Create HDF file --> python hdf5generator.py --create_hdf <HDF path>")
    print("Get datasets from HDF file --> python hdf5generator.py --get_datasets <HDF path>")
    print("Get fttributes from HDF file --> python hdf5generator.py --get_attributes <HDF path>")
    print("Delete groups from HDF file --> python hdf5generator.py --delete_groups <HDF path>")
    print("Create RDF file --> python hdf5generator.py --create_rdf <HDF path>")
    print("search RDF file --> python hdf5generator.py --search_rdf <HDF path>")


if __name__ == "__main__":
    if len(sys.argv) > 2:
        out_file = sys.argv[2]
        if sys.argv[1] == "--create_group":
            input_files = input(
                "Enter file paths (seperated by a space): "
            ).replace('\\', '/')
            in_files = input_files.split(' ')
            groupname = input("Enter new groupname: ")
            write_groups(out_file, groupname)
        elif sys.argv[1] == "--create_hdf":
            call(["rm " + out_file], shell=True)
            groups = []
            input_files = input(
                "Enter file paths (seperated by a space): "
            ).replace('\\', '/')
            input_groups = input(
                "Enter groups (seperated by a space) i.e. /Project/Investigation/Study/Assay/: ")
            in_files = input_files.split(' ')
            groups = input_groups.split(' ')
            write_func(in_files, out_file, groups)
        elif sys.argv[1] == "--get_datasets":
            with h5py.File(out_file, 'r') as f:
                f.visititems(find_datasets)
        elif sys.argv[1] == "--get_attributes":
            get_attr(out_file)
        elif sys.argv[1] == "--delete_groups":
            groups_to_delete = generate_groups_to_delete()
            delete_groups(out_file, groups_to_delete)
        elif sys.argv[1] == "--create_rdf":
            generate_rdf(out_file)
        elif sys.argv[1] == "--search_rdf":
            predicate = input("Enter predicate to search in RDF: ")
            query_rdf(out_file, predicate)
        else:
            help()
    else:
        if sys.argv[1] == "--help":
            help()
    
