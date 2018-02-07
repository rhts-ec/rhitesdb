from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'dash_malaria_compliance\.php', views.malaria_compliance, name='malaria_compliance'),
    url(r'dash_malaria_quarterly\.php', views.ipt_quarterly, name='ipt_quarterly'),
    url(r'dash_malaria_quarterly\.xls', views.ipt_quarterly, {'output_format': 'EXCEL'}, name='ipt_quarterly_excel'),
    url(r'validation_rule\.php', views.validation_rule, name='validation_rule'),
    url(r'data_workflow_new.php', views.data_workflow_new, name='data_workflow_new'),
    url(r'data_workflow.php', views.data_workflow_detail, name='data_workflow_detail'),
    url(r'data_workflows.php', views.data_workflow_listing, name='data_workflow_listing'),
    url(r'data_element_alias.php', views.data_element_alias, name='data_element_alias'),
    url(r'dash_hts_sites.php', views.hts_by_site, name='hts_sites'),
    url(r'dash_hts_districts.php', views.hts_by_district, name='hts_districts'),
    url(r'dash_vmmc_sites.php', views.vmmc_by_site, name='vmmc_sites'),
    url(r'dash_lab_sites.php', views.lab_by_site, name='lab_sites'),
]
