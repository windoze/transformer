from feathr.definition.feature import Feature
from feathr.definition.feature_derivations import DerivedFeature
from feathr.definition.transformation import ExpressionTransformation, WindowAggTransformation
from expr_parser import collect
from supported_functions import SUPPORTED_FUNCTIONS
from typing import List

def gen_dsl(name: str, features: List[Feature]):
    """Generate a dsl file for the given features"""
    
    layers = []
    
    # Add all upstreams to the current_features
    current_features = features.copy()
    while True:
        cf = current_features.copy()
        for f in current_features:
            if isinstance(f, DerivedFeature):
                for uf in f.input_features:
                    if uf not in current_features:
                        cf.append(uf)
        if len(cf) == len(current_features):
            break
        current_features = cf
    
    feature_names = set([f.name for f in current_features])
    
    # Topological sort the features
    while current_features:
        current_layer = set()
        for f in current_features:
            if isinstance(f, Feature):
                # Anchor feature doesn't have upstream
                current_features.remove(f)
                current_layer.add(f)
            elif isinstance(f, DerivedFeature):
                pending = False
                # If any of the input features is still left, it shall wait
                for input in f.input_features:
                    if input in current_features:
                        pending = True
                        break
                if not pending:
                    current_features.remove(f)
                    current_layer.add(f)
        layers.append(current_layer)
        
    identifiers = set()
    stages = []
    for l in layers:
        t = []
        for f in l:
            expr = ""
            if isinstance(f.transform, ExpressionTransformation):
                expr = f.transform.expr
            elif isinstance(f.transform, str):
                expr = f.transform
            elif isinstance(f.transform, WindowAggTransformation):
                raise NotImplementedError(f"Feature '{f.name}' is using WindowAggTransformation, which is not supported")
            t.append(f"{f.name} = {expr}")
            names = set()
            functions = set()
            collect(expr, functions, names)
            for n in names:
                if n not in feature_names:
                    identifiers.add(n)
            for func in functions:
                if func not in SUPPORTED_FUNCTIONS:
                    raise NotImplementedError(f"Feature '{f.name}' uses unsupported function '{func}'")
        stages.append(f'| project {", ".join(t)}')
    stages.append(f'| project-keep {", ".join([f.name for f in features])}', )
    schema = f'({", ".join(identifiers)})'
    return "\n".join([f'{name}{schema}', "\n".join(stages), ";"])
