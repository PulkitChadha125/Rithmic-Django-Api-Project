from django.urls import path
from .views import RithmicApiView

urlpatterns = [
    path('run-rithmic/', RithmicApiView.as_view(), name='run-rithmic'),
]
