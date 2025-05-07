import random # Importando o random
import string # Importando, tem listas de letras, números e caracteres.

user_id= list(range(1, 131)) # user_id terá uma lista de um range de 1 a 130, o ultimo é exclusivo em Python
recipient_id = list(range(1, 131)) # recipient_id recebe uma lista de um range de 1 a 130, o último é exclusivo em Python

def gerador_mensagem() -> dict: # Função com nome de 'gerador_mensagem'
    random_user_id = random.choice(user_id) # Escolher um ID aleatório da lista, ou seja, quem vai recbeer a mensagem.

    copia_recipient = recipient_id.copy() # Fazendo cópia para que quando alteramos não afete a originial.

    copia_recipient.remove(random_user_id) # Remove da lista de destino os mesmo ID'S do remetente, para ele não enviar mensagem a sí mesmo 
    random_recipient_id = random.choice(copia_recipient) # Agora escolher um destinátario aleatório

    mensagem = ''.join(random.choice(string.ascii_letters) for i in range(32)) # Cria uma mensagem aleatória de 32 letras. Usa letras maiúsculas e minúsculas de A a Z.


    return { # Retorna um dicionário com user_id, recipiente_id, mensagem.
        'user_id': random_user_id,
        'recipiente_id': random_recipient_id,
        'mensagem': mensagem
    }   

#Testando.
if __name__ == '__main__': #
    print(gerador_mensagem())

    