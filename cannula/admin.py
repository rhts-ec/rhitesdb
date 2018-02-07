from django.contrib import admin

from mptt.admin import MPTTModelAdmin

from .models import SourceDocument, OrgUnit, DataElement, DataValue, Category, CategoryCombo, ValidationRule, load_excel_to_datavalues, load_excel_to_validations

def load_document_values(modeladmin, request, queryset):
    for doc in queryset:
        all_values = load_excel_to_datavalues(doc)
        for site_name, site_vals in all_values.items():
            DataValue.objects.bulk_create(site_vals)

load_document_values.short_description = 'Load data values from document into DB'

def load_document_validations(modeladmin, request, queryset):
    for doc in queryset:
        load_excel_to_validations(doc)

load_document_validations.short_description = 'Load validation rules from document into DB'

class SourceDocumentAdmin(admin.ModelAdmin):
    readonly_fields = ('orig_filename',)
    list_display = ['uploaded_at', 'orig_filename']
    ordering = ['uploaded_at']
    actions = [load_document_values, load_document_validations]

class OrgUnitAdmin(MPTTModelAdmin):
    list_display = ['name', 'level']

class DataElementAdmin(admin.ModelAdmin):
    list_display = ['name', 'alias', 'value_type']

class CategoryComboAdmin(admin.ModelAdmin):
    filter_horizontal = ['categories']

class DataValueAdmin(admin.ModelAdmin):
    list_display = ['data_element', 'category_combo', 'site_str', 'org_unit', 'month', 'quarter', 'year', 'numeric_value']
    list_filter = ('data_element__name',)
    search_fields = ['data_element__name', 'category_combo__name', 'site_str']

class ValidationRuleAdmin(admin.ModelAdmin):
    list_display = ['name', 'expression']
    filter_horizontal = ['data_elements']

admin.site.register(SourceDocument, SourceDocumentAdmin)
admin.site.register(OrgUnit, OrgUnitAdmin)
admin.site.register(DataElement, DataElementAdmin)
admin.site.register(DataValue, DataValueAdmin)
admin.site.register(Category)
admin.site.register(CategoryCombo, CategoryComboAdmin)
admin.site.register(ValidationRule, ValidationRuleAdmin)

admin.site.site_title = 'RHITES-EC Data Validation Administrative Interface'
admin.site.site_header = 'RHITES-EC Data Validation Admin'
admin.site.index_title = 'System Configuration & Administration'
