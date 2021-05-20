from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute
import time
class UserModel(Model):
    """
    A DynamoDB User
    """
    class Meta:
        table_name = "testing-ishan7"
    email = UnicodeAttribute(null=True)
    first_name = UnicodeAttribute(range_key=True)
    last_name = UnicodeAttribute(hash_key=True)

UserModel.create_table(read_capacity_units=1, write_capacity_units=1,wait=True)

user = UserModel("ishan", "gupta")
user.email = "djohn8@company.org"
user.save()