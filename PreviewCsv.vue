<template>
    <div class="container">
        <div>
            <h2>Preview CSV File</h2>
            <hr/>
            <label>File
                <input type="file" accept=".csv" @change="handleFileUpload( $event )"/>
            </label>
            <br>
            <table v-if="parsed" style="width: 100%;">
                <thead>
                    <tr>
                        <th v-for="hdr in colhdrs"
                        v-bind:key="'colhdr-'+hdr.key">
                            {{ hdr.text }}
                        </th>
                    </tr>
                </thead>
                <tbody>
                    <tr v-for="row in colvals"
                        v-bind:key="'row-'+row.key">
                            <td v-for="column in colhdrs"
                                v-bind:key="'row-'+row.key+'-column-'+column.key">
                                    <input v-model="row[column.value]"/>
                            </td>
                    </tr>
                </tbody>
            </table>
            <br>
            <!--
            <table v-if="parsed" style="width: 100%;">
                <thead>
                    <tr>
                        <th v-for="header in headers"
                        v-bind:key="'header-'+header.key">
                            {{ header.text }}
                        </th>
                    </tr>
                </thead>
                <tbody>
                    <tr v-for="row in desserts"
                        v-bind:key="'row-'+row.key">
                            <td v-for="column in headers"
                                v-bind:key="'row-'+row.key+'-column-'+column.key">
                                    <input v-model="row[column.value]"/>
                            </td>
                    </tr>
                </tbody>
            </table>
            <br>
            -->
            <table v-if="parsed" style="width: 100%;">
                <thead>
                    <tr>
                        <th v-for="(header, key) in content.meta.fields"
                            v-bind:key="'header-'+key">
                            {{ header }}
                        </th>
                    </tr>
                </thead>
                <tbody>
                    <tr v-for="(row, rowKey) in content.data"
                        v-bind:key="'row-'+rowKey">
                            <td v-for="(column, columnKey) in content.meta.fields"
                                v-bind:key="'row-'+rowKey+'-column-'+columnKey">
                                    <input v-model="content.data[rowKey][column]"/>
                            </td>
                    </tr>
                </tbody>
            </table>
            <br>
            <button v-on:click="submitFile()">Submit File</button>
            <button v-on:click="submitUpdates()">Submit Updates</button>
        </div>
    </div>
</template>

<script>
import Papa from 'papaparse';
import axios from 'axios';

export default {
    data(){
        return {
            file: '',
            content: [],
            parsed: false,
            singleval: '',
            colhdrs: [
                {text: 'Name', key: 0, align: 'start', filterable: false, value:'text'},
                {text: 'Type Check', key: 1, value:'typechk'},
                {text: 'Null Check', key: 2, value:'nilchk'},
                {text: 'Empty Check', key: 3, value:'emptychk'},
              ],
            colvals: [],
            typechkvals: [
                {text: 'INTEGER', key: 0},
                {text: 'LONG', key: 1},
                {text: 'FLOAT', key: 2},
                {text: 'DOUBLE', key: 3},
                {text: 'DATE', key: 4},
                {text: 'STRING', key: 5},
            ],
            onoffchkvals: [
                {text: '0', key: 0},
                {text: '1', key: 1},
            ],
            headers: [
                {
                  text: 'Dessert (100g serving)',
                  key: 0,
                  align: 'start',
                  filterable: false,
                  value: 'name'
                },
                { text: 'Calories', key: 1, value: 'calories' },
                { text: 'Fat (g)', key: 2, value: 'fat' },
                { text: 'Carbs (g)', key: 3, value: 'carbs' },
                { text: 'Protein (g)', key: 4, value: 'protein' },
                { text: 'Iron (%)', key: 5, value: 'iron' }
              ],
            desserts: [
                {
                  key: 0,
                  name: 'Frozen Yogurt',
                  calories: 159,
                  fat: 6.0,
                  carbs: 24,
                  protein: 4.0,
                  iron: '1%'
                },
                {
                  key: 1,
                  name: 'Ice cream sandwich',
                  calories: 237,
                  fat: 9.0,
                  carbs: 37,
                  protein: 4.3,
                  iron: '1%'
                },
                {
                  key: 2,
                  name: 'Eclair',
                  calories: 262,
                  fat: 16.0,
                  carbs: 23,
                  protein: 6.0,
                  iron: '7%'
                },
                {
                  key: 3,
                  name: 'Cupcake',
                  calories: 305,
                  fat: 3.7,
                  carbs: 67,
                  protein: 4.3,
                  iron: '8%'
                },
                {
                  key: 4,
                  name: 'Gingerbread',
                  calories: 356,
                  fat: 16.0,
                  carbs: 49,
                  protein: 3.9,
                  iron: '16%'
                },
                {
                  key: 5,
                  name: 'Jelly bean',
                  calories: 375,
                  fat: 0.0,
                  carbs: 94,
                  protein: 0.0,
                  iron: '0%'
                },
                {
                  key: 6,
                  name: 'Lollipop',
                  calories: 392,
                  fat: 0.2,
                  carbs: 98,
                  protein: 0,
                  iron: '2%'
                },
                {
                  key: 7,
                  name: 'Honeycomb',
                  calories: 408,
                  fat: 3.2,
                  carbs: 87,
                  protein: 6.5,
                  iron: '45%'
                },
                {
                  key: 8,
                  name: 'Donut',
                  calories: 452,
                  fat: 25.0,
                  carbs: 51,
                  protein: 4.9,
                  iron: '22%'
                },
                {
                  key: 9,
                  name: 'KitKat',
                  calories: 518,
                  fat: 26.0,
                  carbs: 65,
                  protein: 7,
                  iron: '6%'
                },
                {
                  key: 10,
                  name: 'Donut',
                  calories: 452,
                  fat: 25.0,
                  carbs: 51,
                  protein: 4.9,
                  iron: '22%'
                }
              ],
        }
    },

    methods: {
        handleFileUpload( event ){
            this.file = event.target.files[0];
            this.parseFile();
        },

        resetHeaderValues(){
            this.colvals = [];
        },


        checkNumerics(input) {
            //let regexPattern = /^-?[0-9]+$/;
            //let result = regexPattern.test(x);

            let chkType = 'STRING';

            if(!isNaN(input)){
            
                if (input.valueOf() == Number.parseInt(input)) {
                    chkType = 'INTEGER';
                }
                else {
                    chkType = 'FLOAT';
                }
            
            } else if (!isNaN(Date.parse(input))) {
                chkType = 'DATE';
            }

            return chkType;
        },


        buildHeaderTable(){
            this.resetHeaderValues();
            let collen = this.content.meta.fields.length;
            for (var i = 0; i < collen; i++) {
              let colhdr = this.content.meta.fields[i].trim();
              let rowval = {key: i, text: colhdr, typechk: 'STRING', nilchk: 0, emptychk: 0}
              let rowlen = this.content.data.length;
              for (var j = 0; j < rowlen; j++) {
                let rv = this.content.data[j][colhdr].trim().toString();
                rowval['typechk'] = this.checkNumerics(rv);
              }
              this.colvals.push(rowval);
            }
        },

        parseFile(){

            Papa.parse( this.file, {
                header: true,
                skipEmptyLines: true,
                quotes: true,
                quoteChar: '"',
                escapeChar: '"',
                transformHeader:function(h) {
                  return h.trim().replaceAll('"', '');
                },
                transform:function(h) {
                  return h.trim().replaceAll('"', '');
                },
                complete: function( results ){
                    this.content = results;
                    this.parsed = true;
                    this.singleval = results.data[0]['Weight'];
                    this.buildHeaderTable();
                }.bind(this)
            } );
        },

        submitFile(){
            let formData = new FormData();
            
            formData.append('file', this.file);
            
            axios.post( '/preview-file',
                formData,
                {
                    headers: {
                            'Content-Type': 'multipart/form-data'
                    }
                }
            ).then(function(){
                console.log('SUCCESS!!');
            })
            .catch(function(){
                console.log('FAILURE!!');
            });
        },

        submitUpdates(){  
            axios.post( '/preview-file-changes',
                this.content.data
            ).then(function(){
                console.log('SUCCESS!!');
            })
            .catch(function(){
                console.log('FAILURE!!');
            });
        }
    }
}
</script>

