'use strict';
var ConfigPanel = (function (my) {

    let panelRoot;
    let configItems
    let renderers;

    my.initPanel = function (root) {
        if (!root) return;

        configItems = [];
        renderers = {};
        panelRoot = root;
        initRenderers();
        init(root);
    }

    my.values = function () {
        var retVal = {};
        configItems.filter(function(item) {
            return shouldRender(item);
        }).forEach(function(item) {
            retVal[item.propertyBinding] = resolveValue(panelRoot, item);
        });
        return retVal;
    }

    function initRenderers() {
        renderers['java.lang.Boolean'] = initBooleanRenderer();
        renderers['java.lang.Integer'] = initIntegerRenderer();
    }

    function initBooleanRenderer() {
        var retVal = {};
        retVal.render =  function(root, item) {
            var container = $("<div></div>").attr("class", "onoffswitch-large");
            var input = $("<input></input>").attr("type", "checkbox").attr("class", "onoffswitch-checkbox").attr("id", `${item.id}`);
            input.prop("checked", item.initialValue);
            checkDisabled(root, item);

            input.change(function() {
                configItems.forEach(function(_item) {
                    var _container = resolveContainerByItemId(_item.id);
                    if(item.id !== _item.id) {
                        checkDisabled(_container, _item);
                    }
                    else {
                        _item.initialValue = _container.is(":checked");
                    }
                })
            });

            var label = $("<label></label>").attr("class", "onoffswitch-label").attr("for", `${item.id}`);
            var innerContainer = $("<div></div>").attr("class", "onoffswitch-inner").attr("_on","ENABLED").attr("_off","DISABLED");
            var switchContainer = $("<div></div>").attr("class", "onoffswitch-switch-large");

            label.append(innerContainer);
            label.append(switchContainer);
            container.append(input);
            container.append(label);

            root.append(container);

        }
        retVal.element = function(root, item) {
            return $(`#${item.id}`);
        }
        retVal.resolve = function(root, item) {
            return retVal.element(root, item).is(":checked");
        }
        return retVal;
    }

    function initIntegerRenderer() {
        var retVal = {};
        retVal.render = function(root, item) {
            var input = $("<input></input>").attr("type", "number").attr("id", `${item.id}`).attr("step", "1").attr("min", "1").attr("value", `${item.initialValue}`);
            checkDisabled(root, item);
            root.append(input);
        }
        retVal.element = function(root, item) {
            return $(`#${item.id}`);
        }
        retVal.resolve = function(root, item) {
            return retVal.element(root, item).val();
        }
        return retVal;
    }

    function shouldRender(item) {
        return item.renderIf.interpolate({
          item: item,
          configItems: configItems,
          getItemById: resolveItemById,
          getValueByItemId: function(item) {
            return resolveValueByItemId(configPanel, item);
          }
       }) === 'true';
    }

    function checkDisabled(input, item) {
        if(shouldRender(item)) {
            input.show();
        } else {
            input.hide();
        }
    }

    function init(root) {
        var token = $("meta[name='_csrf']").attr("content");
        var url = root.attr('data-configPanelDataUrl');
        $.ajax({
            url: url,
            type: 'GET',
            headers: {
                'Accept': 'application/json',
                'X-CSRF-TOKEN': token
            },
            success: function (data) {
                configItems = data.items;
                if(configItems) {
                    configItems.forEach(function(item) {
                        renderItem(root, item);
                    });
                }
            },
            error: function(xhr, status, error) {
                console.error('Could not get panel config data');
            }
        });
    }

    function renderItem(configPanel, item) {
        var container = $("<div></div>").attr("id", `container-${item.id}`).attr("class", "span-4");
        var title = $(`<p><span class="placeholder">${item.name}:</span></p>`);
        container.append(title);
        configPanel.append(container);
        var renderer = renderers[item.type];
        renderer.render(container, item);
    }

    function resolveValue(configPanel, item) {
        return renderers[item.type].resolve(configPanel, item);
    }

    function resolveValueByItemId(configPanel, id) {
        var item = resolveItemById(id);
        return renderers[item.type].resolve(configPanel, item);
    }

    function resolveItemById(id) {
        return configItems.filter(function(item) {
            return item.id === id;
        })[0];
    }

    function resolveContainerByItemId(id) {
        return $(`#container-${id}`);
    }

    function resolveRendererByItem(id) {
        return configItems.filter(function(item) {
            return item.id === id;
        })[0];
    }

    String.prototype.interpolate = function(params) {
      const names = Object.keys(params);
      const values = Object.values(params);
      return new Function(...names, `return \`${this}\`;`)(...values);
    }


    return my;
}(ConfigPanel || {}));

