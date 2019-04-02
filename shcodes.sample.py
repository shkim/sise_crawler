# insert your code below
codelist = '''
069500
099140
'''

def get_shcodes():
    return list(filter(lambda s: s, map(lambda x: x.strip(), codelist.split('\n'))))

if __name__ == "__main__":
    print(get_shcodes())
