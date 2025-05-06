import random
import string

user_id= list(range(1, 131))
recipient_id = list(range(1, 131))

def gerador_mensagem() -> dict:
    random_user_id = random.choice(user_id)

    copia_recipient = recipient_id.copy()

    copia_recipient.remove(random_user_id)
    random_recipient_id = random.choice(copia_recipient)

    mensagem = ''.join(random.choice(string.ascii_letters) for i in range(32))


    return {
        'user_id': random_user_id,
        'recipiente_id': random_recipient_id,
        'mensagem': mensagem
    }   

#Testando.
if __name__ == '__main__':
    print(gerador_mensagem())