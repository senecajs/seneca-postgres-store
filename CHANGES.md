## 2.0.0: 2016-04-05

Removed the CamelCase names to Snake case conversion that the plugin was making internally. Backward compatibility is ensured by allowing the user to pass as options two functions named toColumnName() and fromColumnName() that make this conversion. More details are provided in the [seneca-standard-query](https://github.com/senecajs/seneca-standard-query) **Column name transformation, backward compatibility section**.

All query generation code related to basic seneca functionality was moved to [seneca-standard-query](https://github.com/senecajs/seneca-standard-query) and the extended query functionality moved to [seneca-store-query](https://github.com/senecajs/seneca-store-query). This doesn't change functionality but enables functionality reuse into other stores.
