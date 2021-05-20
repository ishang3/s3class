import requests



class RequestData():
    def __init__(self,q):
        self.q = q 
        self.response = requests.get("http://dev.getsensable.com/api/v1/system-token/1")
        self.token = self.response.json()['token']
        self.get_setups()


    def get_setups(self,):
        headers = {
            "Authorization" : f"Bearer {self.token}"
        }
        response2 = requests.get("http://dev.getsensable.com/api/v1/company/name/ishan",headers=headers)

        for res in response2.json():
            print('***************')
            print(res)
    
        




