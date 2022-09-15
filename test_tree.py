from pathlib import Path
import re
from neoj4 import Neo4jConnection
import json

database = 'tree'
path_name = 'Stored Procedures/'


# проверить список используемых запросов в ms
def replace_char(s: str) -> str:
    return ''.join(re.findall(r'[\w_\.\s]', s))


def replace_box(s: str) -> str:
    return s.replace('[', '').replace(']', '')


def create_select_find_proc(as_name: str, name_t: str, name_proc: str) -> str:
    return f"\nMATCH ({as_name}:{name_t}) WHERE {as_name}.name = '{name_proc}'"


def create_merge_proc(list_proc, as_name, name_t, conn, query_1, query_3):
    for i in list_proc:
        if i == '':
            continue
        query_2 = create_select_find_proc(as_name, name_t, i)
        conn.query(query_1 + query_2 + query_3, db=database)


def replace_doble_probel(s: str) -> str:
    while s.find('  ') > -1:
        s = s.replace('  ', ' ')
    return s


class StrPars:
    def __init__(self, text):
        self.insert_list = None
        self.update_list = None
        self.exec_list = None
        self.description = None
        self.proc_name = None
        self.text = text
        self.find_name()
        self.find_desc()
        self.find_insert()
        self.find_update()
        self.find_exec()

    def replace_comments(self) -> str:
        # этот патр я не понимаю как делать
        # i = 0
        # while self.text.find('/*') < self.text.find('*/'):
        #     self.text = self.text[:self.text.find('/*')] + self.text[self.text.find('*/') + 2:]
        #     i += 1
        # if i > 20:
        #     break

        ss = [i[:i.find('--')].strip() if i.find('--') > -1 else i for i in self.text.split('\n')]
        ss = [i for i in ss if i != '']
        self.text = '\n'.join(ss)

    def find_name(self):
        name = re.findall(r'create\s*procedure\s.+|create\s*proc.+', self.text)
        name = replace_doble_probel(replace_box(name[0])).replace('create procedure ', '')
        if name.find(" ") > -1:
            self.proc_name = name[:name.find(" ")]
        else:
            self.proc_name = name

    def find_desc(self):
        tmp_ = self.text[self.text.find("Description") + 13:]
        self.description = tmp_[:tmp_.find("\n")]

    def find_exec(self):
        self.exec_list = list(set([replace_box(i.replace('exec', '').replace('EXEC', '').strip()) for i in
                                   re.findall(r'\n*[^-{2}*]EXEC\s.+?(?=\n)|\n*[^-{2}*]exec\s.+?(?=\n)', file_text)]))

    def find_update(self):
        self.update_list = list(set([i.replace('UPDATE ', '').replace('update', '').strip() for i in
                                     re.findall(r'\n*UPDATE\s.*?(?=\()|\n*update\s.*?(?=\()', file_text)]))

    def find_insert(self):
        self.insert_list = list(set([i.replace('INSERT INTO', '').replace('insert into', '').strip() for i in
                                     re.findall(r'\n*INSERT\sINTO\s.*?(?=\()|\n*insert\sinto\s.*?(?=\()', file_text)]))


files = sorted(list(map(str, list(Path(path_name).rglob("*.sql")))))
info_full_tree = []
for file in files:
    branch = {}
    with open(file, 'r') as f:
        file_text = f.read().lower()
    # print(file)
    ss = StrPars(file_text)
    ss.replace_comments()
    # name = ss.proc_name
    # file_text = replace_comments(file_text)
    # name = re.findall(r'create\s*procedure\s.+|create\s*proc.+', file_text)
    # name = replace_doble_probel(replace_box(name[0])).replace('create procedure ', '')
    # branch.update({'name': name[:name.find(" ")]})
    branch.update({'name': ss.proc_name})
    # tmp_ = file_text[file_text.find("Description") + 13:]
    # description = tmp_[:tmp_.find("\n")]
    branch.update({'description': ss.description})
    # exec_list = [replace_box(i.replace('exec', '').replace('EXEC', '').strip()) for i in
    #              re.findall(r'\n*[^-{2}*]EXEC\s.+?(?=\n)|\n*[^-{2}*]exec\s.+?(?=\n)', file_text)]
    branch.update({'exec_list': ss.exec_list})
    # insert_list = [i.replace('INSERT INTO', '').replace('insert into', '').strip() for i in
    #                re.findall(r'\n*INSERT\sINTO\s.*?(?=\()|\n*insert\sinto\s.*?(?=\()', file_text)]
    branch.update({'insert_list': ss.insert_list})
    # update_list = [i.replace('UPDATE ', '').replace('update', '').strip() for i in
    #                re.findall(r'\n*UPDATE\s.*?(?=\()|\n*update\s.*?(?=\()', file_text)]
    branch.update({'update_list': ss.update_list})
    info_full_tree.append(branch)

# Это если нужно просто вывести словарик
for branch in info_full_tree:
    debug = []
    jobs = []
    other = []
    for i in branch['exec_list']:
        proc = replace_char(i[:i.find(" ")] if i.find(" ") > -1 else i).strip()
        if i.find('debug') > -1:
            debug.append(proc)
        elif i.find('jobs') > -1:
            jobs.append(proc)
        else:
            other.append(proc)

    branch.update(
        {
            'debug': list(set(debug)),
            'jobs': list(set(jobs)),
            'other': list(set(other))
        }
    )

with open('json_test.json', 'w') as f:
    json.dump(info_full_tree, f, ensure_ascii=False, indent=4)

exit(0)

print('создаем элементы')
conn = Neo4jConnection(uri="neo4j://localhost:7687", user="neo4j", password="test")
conn.query(f"CREATE OR REPLACE DATABASE {database}")
for branch in info_full_tree:
    query = ''
    if branch['name'].find('debug') > -1:
        branch['type'] = 'debug'
        query = "CREATE (d:Debug {name: '%s', description: '%s', type: '%s'});" % (
            branch['name'],
            branch['description'],
            'debug'
        )

    elif branch['name'].find('jobs') > -1:
        branch['type'] = 'jobs'
        query = "CREATE (j:Job {name: '%s', description: '%s', type: '%s'});" % (
            branch['name'],
            branch['description'],
            'jobs'
        )
    else:
        branch['type'] = 'other'
        query = "CREATE (o:Other {name: '%s', description: '%s', type: '%s'});" % (
            branch['name'],
            branch['description'],
            'other'
        )

    conn.query(query, db=database)

print('создаем связи')
for branch in info_full_tree:
    if branch['type'] == 'debug':
        query_1 = create_select_find_proc('x', 'Debug', branch['name'])
    elif branch['type'] == 'jobs':
        query_1 = create_select_find_proc('x', 'Job', branch['name'])
    else:
        query_1 = create_select_find_proc('x', 'Other', branch['name'])

    as_name = 'y'
    query_3 = f"\nCREATE (x) - [:exec] -> ({as_name})"

    create_merge_proc(branch['debug'], as_name, 'Debug', conn, query_1, query_3)
    create_merge_proc(branch['jobs'], as_name, 'Job', conn, query_1, query_3)
    create_merge_proc(branch['other'], as_name, 'Other', conn, query_1, query_3)

    # for i in list(set(branch['exec_list'])):
    #     proc = replace_char(i[:i.find(" ")] if i.find(" ") > -1 else i)
    #
    #     if proc.strip() == '':
    #         continue
    #
    #     if proc.find('debug') > -1:
    #         query_2 = create_select_find_proc('y', 'Debug', proc)
    #     elif proc.find('jobs') > -1:
    #         query_2 = create_select_find_proc('y', 'Job', proc)
    #     else:
    #         query_2 = create_select_find_proc('y', 'Other', proc)
    #     conn.query(query_1 + query_2 + query_3, db=database)

conn.close()
