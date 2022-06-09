from faker import Faker

faker = Faker()

def get_registered_user():
    return{
        'first_name': faker.first_name(),
        'last_name': faker.last_name(),
        'address': faker.address(),
        'created_at': int(faker.year()),
    }

if __name__ == "__main__":
    print(get_registered_user())
    for what in get_registered_user().values():
        print(type(what))