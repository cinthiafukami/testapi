

with open("passFile.txt", "w") as file: 
    file.write('testando\n')
    file.write('linha 2\n')
print("sucesso!")

file = open("passFile.txt", "r")
content = file.read()
print(content)
resources = file.close()