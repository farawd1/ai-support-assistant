"""Provision accounts locally: python -m app.accounts USERNAME ROLE."""
import argparse
from getpass import getpass
from .main import create_account, init_db

def main():
    parser=argparse.ArgumentParser(description='Создать аккаунт Repl.io')
    parser.add_argument('username')
    parser.add_argument('role',choices=['user','operator'])
    args=parser.parse_args()
    password=getpass('Пароль (от 12 символов): ')
    if password!=getpass('Повторите пароль: '): parser.error('Пароли не совпадают')
    init_db()
    create_account(args.username,password,args.role)
    print('Аккаунт создан')

if __name__=='__main__': main()
