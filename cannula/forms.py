from django.forms import ModelForm

from . import models

class SourceDocumentForm(ModelForm):
	class Meta:
		model = models.SourceDocument
		fields = ['file',]


class DataElementAliasForm(ModelForm):
    def __init__(self, *args, **kwargs):
        super(DataElementAliasForm, self).__init__(*args, **kwargs)
        self.fields['name'].widget.attrs['readonly'] = True

    class Meta:
        model = models.DataElement
        fields = ['name', 'alias']
            