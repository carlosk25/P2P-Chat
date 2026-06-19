##controle das conexões TCP e mensagens entre peers

#1. conexões inbound
 #abrir socket local
 #aceitar conexões de outros peers
 #receber HELLO
 #responder HELLO_OK

#2. conexões outbound
 #conectar em outro peer
 #enviar HELLO
 #esperar HELLO_OK
 #manter socket aberto

#abrir servidor TCP local
#aceitar conexões de entrada
#conectar em outros peers
#enviar HELLO
#receber HELLO
#responder HELLO_OK
#guardar sockets conectados
#fechar conexões