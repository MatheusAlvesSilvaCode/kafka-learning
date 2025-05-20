# pedido/urls.py

from django.urls import path
from . import views

urlpatterns = [
    path('order/<int:qtd>', views.fazer_pedido, name='fazer_pedido'),
]
