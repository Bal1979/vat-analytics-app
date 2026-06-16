"""Uafhængig valideringssuite for VAT Analytics.

Hver aktiv kontrol har et scenarie med en REN baseline (kontrollen må ikke fyre)
og en DEFEKT variant med én plantet defekt (kontrollen SKAL fyre). Det beviser
både følsomhed (fanger defekten) og fravær af falske alarmer (ren baseline er tavs).
"""
