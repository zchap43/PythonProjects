import yaml
def read_yaml_file():
    with open('./database_info.yaml', 'r') as file:
        yams = yaml.safe_load(file)
    return yams