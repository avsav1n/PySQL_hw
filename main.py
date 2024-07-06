from random import choice
from random import randint
import logging
import sys
import psycopg2

import config

class PySQL:
    '''
    Класс для работы с базой данных PostgreSQL
    База данных хранит персональную информацию о клиенте
        1. Имя (строка, 30 символов)
        2. Фамилия (строка, 30 символов)
        3. E-mail (строка, 30 символов)
        4. Номер телефона (строка, строго 10 символов, формат 999 999 99 99)
    Взаимодействие с методами класса осуществляется путем передачи частично 
    заполненного словаря params с имеющейся информацией о клиенте, вида:
        {'client_id': int, 
         'name': str, 
         'surname': str, 
         'mail': str, 
         'number': int или [int, ...],
         'new_name': str,
         'new_surname': str,
         'new_mail': str,
         'new_number': int или [int, ...]}
    '''
    def __init__(self, database:str, user:str, password:str):
        self.database = database
        self.user = user
        self.password = password
        try:
            self.connection = self._psyconnect()
        except UnicodeDecodeError as error:
            logging.error(f'Ошибка подключения к базе данных "{self.database}" - {error}')
            sys.exit()
        else:
            logging.info(f'Успешное подключение к базе данных "{self.database}". '
                         'Объект класса connection создан')
            self.connection.close()

    def __enter__(self):
        '''
        Метод для работы с объектом connect
        библиотеки psycopg2 как с контекстным менеджером
        '''
        self.connection = self._psyconnect()

    def __exit__(self, exception_type, exception_value, traceback):
        '''
        Метод для работы с объектом connect
        библиотеки psycopg2 как с контекстным менеджером
        '''
        self.connection.close()

    def __del__(self):
        'Для красоты'
        logging.info(f'{'-' * 100}')

    def _psyconnect(self) -> psycopg2.connect:
        '''
        Функция создания соединения с базой данных
        '''
        connection = psycopg2.connect(database=self.database,
                                      user=self.user,
                                      password=self.password)
        return connection

    def delete_table(self):
        '''
        Функция удаления таблиц из базы данных
        '''
        logging.info('Запуск функции (delete_table) '
                     'удаления таблиц "phone", "client"')
        with self:
            with self.connection.cursor() as cursor:
                for table in 'phone', 'client':
                    try:
                        cursor.execute(f'''DROP TABLE {table};''')
                        self.connection.commit()
                    except (psycopg2.errors.UndefinedTable, psycopg2.errors.InFailedSqlTransaction):
                        logging.warning('При удалении возникла ошибка - '
                                        f'таблицы "{table}" не существует')
                    else:
                        logging.info(f'Таблица "{table}" успешно удалена')

    def create_table(self):
        '''
        Функция, создающая структуру базы данных (п. 1)
        '''
        logging.info('Запуск функции (create_table) '
                     'создания таблиц "client" и "phone"')
        with self:
            with self.connection.cursor() as cursor:
                cursor.execute('''
                                CREATE TABLE IF NOT EXISTS client(
                                    client_id SERIAL primary key,
                                    name VARCHAR(30) NOT NULL,
                                    surname VARCHAR(30) NOT NULL,
                                    mail VARCHAR(30) NOT NULL UNIQUE);
                                ''')
                cursor.execute('''
                                CREATE TABLE IF NOT EXISTS phone(
                                    phone_id SERIAL primary key,
                                    client_id INTEGER REFERENCES client(client_id),
                                    number VARCHAR(10) UNIQUE CHECK(char_length(number) = 10));
                                ''')
                self.connection.commit()
                logging.info('SUCCESS: Таблицы "client" и "phone" созданы')

    def add_client(self, params:dict):
        '''
        Функция добавления информации о клиенте (п. 2)
        Подразумевается работа с таблицами "client" (всегда) и "phone" (опционально)
        Для добавления информации о клиенте, словарь params обязательно должен иметь значения
        по ключам 'name', 'surname' и 'mail'
        '''
        logging.info('Запуск функции (add_client) '
                     f'добавления информации: {params}')
        try:
            name = params['name']
            surname = params['surname']
            mail = params['mail']
            numbers = params.get('number')
        except KeyError:
            logging.warning("Ошибка ввода данных - "
                            "параметры 'name', 'surname' и 'mail' являются обязательными")
            return
        with self:
            with self.connection.cursor() as cursor:
                cursor.execute(f'''
                                INSERT INTO client(name, surname, mail)
                                VALUES ('{name.title()}', '{surname.title()}', '{mail}')
                                RETURNING client_id;
                                ''')
                client_id = cursor.fetchone()[0]
                self.connection.commit()
        logging.info(f'SUCCESS: Информация о клиенте {surname} {name} добавлена в таблицу '
                     f'"client". Идентификатор клиента - {client_id}')
        if numbers:
            params = {'new_number': numbers}
            self.add_phone(params, _existance=client_id)

    def add_phone(self, params:dict, _existance=None):
        '''
        Функция добавления номера телефона (п. 3)
        Подразумевается работа с таблицей "phone"
        Для добавления информации, в словарь params, помимо идентификационной информации, 
        необходимо внести дополнительный параметр по ключу 'new_number'
        Параметр _existance может принимать значение client_id, 
        в таком случае пропускается поиск клиента в таблице "client"
        '''
        logging.info('Запуск функции (add_phone) '
                     f'добавления номера телефона: {params}')
        client_id = _existance if _existance else self.find_client(params, _id_only=True)
        if not client_id:
            return
        if isinstance(numbers := params.get('new_number'), int):
            numbers = [numbers]
        phones, denial = [], []
        with self:
            with self.connection.cursor() as cursor:
                for number in numbers:
                    if len(str(number)) >= 10:
                        try:
                            cursor.execute(f'''
                                            INSERT INTO phone(client_id, number)
                                            VALUES ('{client_id}', '{str(number)[-10:]}');
                                            ''')
                            self.connection.commit()
                            phones.append(number)
                        except psycopg2.errors.UniqueViolation:
                            logging.warning(f'Номер {number} уже существует в базе данных')
                    else:
                        denial.append(number)
                if denial:
                    logging.warning('Ошибка ввода номера(ов) '
                                    f'для добавления: {', '.join(map(str, denial))}')
                if phones:
                    logging.info(f'SUCCESS: Номер(а) {', '.join(map(str, phones))} '
                                 'добавлен(ы) в таблицу "phone"')

    def change_client(self, params:dict):
        '''
        Функция изменения данных о клиенте (п. 4)
        Подразумевается работа с таблицей "client" и "phone" (опционально)
        Для изменения информации, в словарь params, помимо идентификационной информации, 
        необходимо добавить дополнительные параметры по ключам 'new_name', 'new_surname', 
        'new_mail', 'new_number' (что будет, то и поменяется)
        В связи с тем, что к одному клиенту могут быть привязаны несколько номеров,
        в словаре params, в качестве значения ключа 'number' обязательно
        необходимо указать номер телефона, который будет заменен на значение ключа 'new_number'
        '''
        logging.info('Запуск функции (change_client) '
                     f'изменения информации: {params}')
        client_id = self.find_client(params, _id_only=True)
        if not client_id:
            return
        new_params = {key[4:]:value for key, value in params.items() if 'new_' in key}
        flag = 0
        with self:
            with self.connection.cursor() as cursor:
                for key, value in new_params.items():
                    if key == 'number' and params.get('number'):
                        if len(str(value)) < 10:
                            logging.warning(f'Ошибка ввода номера: {value}')
                            continue
                        cursor.execute(f'''
                                       SELECT phone_id
                                        FROM phone
                                        WHERE number = '{params['number']}';
                                        ''')
                        if phone_id := cursor.fetchone():
                            phone_id = phone_id[0]
                            try:
                                cursor.execute(f'''
                                                UPDATE phone
                                                SET {key} = '{str(value)[-10:]}'
                                                WHERE phone_id = {phone_id}
                                                ''')
                                flag += 1
                                self.connection.commit()
                            except psycopg2.errors.UniqueViolation:
                                logging.warning(f'Номер {str(value)[-10:]} уже существует в '
                                                'базе данных')
                    else:
                        try:
                            cursor.execute(f'''
                                            UPDATE client
                                            SET {key} = '{value}'
                                            WHERE client_id = {client_id};
                                            ''')
                            flag += 1
                            self.connection.commit()
                        except psycopg2.errors.UniqueViolation:
                            logging.warning(f'Адрес почты {value} уже существует в базе данных')
        if flag:
            logging.info(f'SUCCESS: Информация о клиенте обновлена')

    def delete_phone(self, params:dict, _all_numbers=False):
        '''
        Функция удаления телефона (п. 5)
        Подразумевается работа с таблицей "phone"
        При _all_numbers = True, params принимает значение client_id - 
        в таком случае осуществляется удаление всех телефонных номеров по client_id из таблицы
        Удаляются номера, являющиеся значениями ключа 'number' словаря params
        '''
        if _all_numbers:
            client_id = params
            logging.info('Запуск функции (delete_phone) удаления '
                         f'всех номеров телефона по идентификатору: {client_id}')
        else:
            if numbers := params.get('number'):
                if isinstance(numbers, int):
                    numbers = [numbers]
                logging.info('Запуск функции (delete_phone) удаления '
                             f'номеров телефона: {params}')
            else:
                logging.warning(f'Ошибка входных данных: {numbers}')
                return
        with self:
            with self.connection.cursor() as cursor:
                if _all_numbers:
                    cursor.execute(f'''
                                    DELETE FROM phone
                                    WHERE client_id = {client_id} RETURNING *;
                                    ''')
                    if cursor.fetchone():
                        logging.info('SUCCESS: Информация о номерах телефона клиента '
                                     f'({client_id}) удалена из таблицы "phone"')
                    else:
                        logging.info(f'Информация о номерах телефона клиента ({client_id}) '
                                     'не найдена')
                    self.connection.commit()

                    return
                phones = []
                for number in numbers:
                    cursor.execute(f'''
                                    DELETE FROM phone
                                    WHERE number = '{number}' RETURNING number;
                                    ''')
                    if phone := cursor.fetchone():
                        phones.append(phone[0])
                self.connection.commit()
        if phones:
            logging.info(f'SUCCESS: Из таблицы "phone" удален(ы) номер(а): {' '.join(phones)}')
        else:
            logging.info('В таблице "phone" данные номера отсутствуют')

    def delete_client(self, params:dict):
        '''
        Функция удаления информации о клиенте (п. 6)
        Подразумевается работа с таблицами "client" (всегда) и "phone" (опционально)
        Удаление осуществляется по какой-либо идентификационной информации,
        переданной в словаре params: 'client_id', 'name' и 'surname', 'mail', 'number'
        '''
        logging.info('Запуск функции (delete_client) '
                     f'удаления информации: {params}')
        client_id = (params['client_id'] if params.get('client_id')
                     else self.find_client(params, _id_only=True))
        self.delete_phone(client_id, _all_numbers=True)
        with self:
            with self.connection.cursor() as cursor:
                cursor.execute(f'''
                                DELETE FROM client
                                WHERE client_id = {client_id} RETURNING *;
                                ''')
                if cursor.fetchone():
                    logging.info(f'SUCCESS: Информация о клиенте ({client_id}) '
                                 'удалена из таблицы "client"')
                else:
                    logging.info('Информация о клиенте отсутствует')
                self.connection.commit()

    def find_client(self, params:dict, _id_only=False) -> dict:
        '''
        Функция поиска данных о клиенте по введенным параметрам (п. 7)
        Параметр params принимает принимает словарь params с имеющимися данными о клиенте
        Полноценный поиск осуществляется при наличии следующей идентификационной информации:
            или 'client_id',
            или 'name' и 'surname',
            или 'mail',
            или 'number'
        Если _id_only = True, функция вернет только значение client_id
        Если _id_only = False (по-умолчанию), функция вернет словарь с полной информацией о клиенте 
        '''
        logging.info('Запуск функции (find_client) '
                     f'поиска информации: {params}')
        if _id_only and params.get('client_id'):
            return params['client_id']
        if client_id := params.get('client_id'):
            info = self._find_client_w_id(client_id)
        elif mail := params.get('mail'):
            info = self._find_client_w_mail(mail)
        elif params.get('number'):
            number = params['number'][0] if isinstance(params['number'], list) else params['number']
            info = self._find_client_w_numbers(number)
        elif params.get('name') and params.get('surname'):
            info = self._find_client_w_name(params['name'], params['surname'])
        else:
            logging.warning('Недостаточно данных для поиска информации о клиенте в базе данных. '
                            'Дальнейший поиск невозможен')
            return
        if info:
            tmp, *_ = info
            if _id_only:
                return tmp[0]
            result = dict(zip(('client_id', 'name', 'surname', 'mail'), tmp))
            result['number'] = [tup[4] for tup in info if tup[4]]
            logging.info(f'SUCCESS: Информация о клиенте: {result}')
            return result

    def _find_client_w_id(self, client_id):
        '''
        Функция поиска информации о клиенте по идентификатору
        '''
        logging.info(f'Выполняется поиск информации по идентификатору клиента: {client_id}')
        with self:
            with self.connection.cursor() as cursor:
                cursor.execute(f'''
                                SELECT c.client_id, name, surname, mail, number
                                FROM client AS c 
                                LEFT JOIN phone AS p ON c.client_id = p.client_id 
                                WHERE c.client_id = {client_id};
                                ''')
                if result := cursor.fetchall():
                    return result
                logging.warning(f'По идентификатору ({client_id}) клиент не найден')

    def _find_client_w_numbers(self, number):
        '''
        Функция поиска информации о клиенте по номеру(ам) телефона
        '''
        logging.info(f'Выполняется поиск информации по номеру телефона: {number}')
        with self:
            with self.connection.cursor() as cursor:
                cursor.execute(f'''
                                SELECT c.client_id, name, surname, mail, number
                                FROM client AS c 
                                LEFT JOIN phone AS p ON c.client_id = p.client_id 
                                WHERE number = '{number}';
                                ''')
                if result := cursor.fetchone():
                    client_id = result[0]
                    return self._find_client_w_id(client_id)
                logging.warning(f'По номеру телефона ({number}) клиент не найден')

    def _find_client_w_mail(self, mail):
        '''
        Функция поиска информации о клиенте по электронной почте
        '''
        logging.info(f'Выполняется поиск информации по адресу электронной почты: {mail}')
        with self:
            with self.connection.cursor() as cursor:
                cursor.execute(f'''
                                SELECT c.client_id, name, surname, mail, number
                                FROM client AS c 
                                LEFT JOIN phone AS p ON c.client_id = p.client_id 
                                WHERE c.mail = '{mail}';
                                ''')
                if result := cursor.fetchall():
                    return result
                logging.warning(f'По адресу электронной почты ({mail}) клиент не найден')

    def _find_client_w_name(self, name, surname):
        '''
        Функция поиска инфомации о клиенте по имени и фамилии
        '''
        logging.info(f'Выполняется поиск информации по фамилии и имени клиента: {surname} {name}')
        with self:
            with self.connection.cursor() as cursor:
                cursor.execute(f'''
                                SELECT c.client_id, name, surname, mail, number
                                FROM client AS c 
                                LEFT JOIN phone AS p ON c.client_id = p.client_id 
                                WHERE c.name = '{name}' AND c.surname = '{surname}';
                                ''')
                if result := cursor.fetchall():
                    return result
                logging.warning(f'По фамилии и имени ({surname} {name}) клиент не найден')


def init_logging():
    '''
    Функция настройки модуля logging
    '''
    log_in_file = logging.FileHandler(r'progress.log', mode='a', encoding='utf-8')
    log_in_console = logging.StreamHandler()
    logging.basicConfig(level=logging.INFO, handlers=(log_in_console, log_in_file),
                        format='%(asctime)s %(levelname)s %(message)s')

def rand_info(parameter=None):
    '''
    Функция генерации параметров для работы с базой данных
    '''
    names = ('Алексей', 'Иван', 'Владимир', 'Олег', 'Виктор', 'Даниил',
             'Николай', 'Виталий', 'Сергей', 'Александр', 'Игорь', 'Никита')
    surnames = ('Савин', 'Морозов', 'Сидоров', 'Иванов', 'Петров', 'Чижиков',
                'Никитин', 'Маринин', 'Касьянов', 'Мельников', 'Ткачов', 'Фролов')
    mails = ('@mail', '@yandex', '@gmail', '@cloud')
    match parameter:
        case 'name':
            return choice(names)
        case 'surname':
            return choice(surnames)
        case 'mail':
            return f'{randint(1, 99)}{choice(mails)}'
        case 'number':
            return randint(1000000000, 9999999999)

init_logging()

pysql = PySQL(database='pypost', user=config.database_name, password=config.database_password)

info = {
    # 'client_id': 1,
    # 'name': rand_info('name'),
    # 'surname': rand_info('surname'),
    # 'mail': rand_info('mail'),
    # 'number': [rand_info('number'), rand_info('number')],
    # 'new_mail': rand_info('mail'),
    # 'new_name': rand_info('name'),
    # 'new_surname': rand_info('surname'),
    # 'new_number': rand_info('number')
}

# pysql.delete_table()
# pysql.create_table()

# pysql.add_client(info)
# pysql.add_phone(info)
# pysql.change_client(info)
# pysql.delete_phone(info)
# pysql.delete_client(info)
# pysql.find_client(info)
