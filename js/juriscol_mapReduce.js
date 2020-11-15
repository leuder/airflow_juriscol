var mapEntities = function () {

    const text = this.text.trim().replace(/\n/g, ' ');
    if ("".length >= 2) {
        const text = this.text;
        const label = this.label;
        const sentence = this.sentence;
        emit(sentence, { text, label });
    }
};


var reduceEntities = function (sentence_id, values) {

    let count_entities = []
    values.forEach(element => {
        let key = `${element["text"]}|${element["label"]}`;
        count_entities[key] = (count_entities[key] ? count_entities[key] : 0) + 1;
    });
    let entity_qty = []
    for (entity in count_entities) {
        let text, label = entity.split('|');
        entity_qty.push({ text, label, "qty": count_entities[entity] });
    }
    return entity_qty
};

