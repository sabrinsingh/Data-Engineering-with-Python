
text = 'My name is Sabrin.'
text_list = text.split(' ')
print(text_list)

text = ' '.join(text_list)
print(text)



GDP_list = ["21,430", "3,860", "2,715", "1,001"]
c=[]

for x in GDP_list:
    a= x.split(',') 
    print(a)
    b=float("".join(a))
    print(b)
    c.append(b)




print(c)
