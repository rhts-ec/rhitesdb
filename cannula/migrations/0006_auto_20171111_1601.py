# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models

def make_default_category_and_categorycombo(apps, schema_editor):
    Category = apps.get_model('cannula', 'Category')
    CategoryCombo = apps.get_model('cannula', 'CategoryCombo')

    def_categ = Category(name='default')
    def_categ.save()
    def_cat_combo = CategoryCombo(name='(default)')
    def_cat_combo.save()
    def_cat_combo.categories.add(def_categ)
    def_cat_combo.save()

class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0005_category_categorycombo'),
    ]

    operations = [
        migrations.RunPython(make_default_category_and_categorycombo),
    ]
